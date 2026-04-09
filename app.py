import os
import json
import re
import struct
import zlib
import hashlib
import secrets
from datetime import datetime
from flask import Flask, request, jsonify, send_file
from werkzeug.utils import secure_filename

app = Flask(__name__, static_folder='.')

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'users')
ALLOWED_EXTENSIONS = {'.txt', '.md', '.csv', '.log', '.json', '.xml', '.html'}

def get_user_dir(user_id):
    d = os.path.join(DATA_DIR, user_id)
    os.makedirs(os.path.join(d, 'uploads'), exist_ok=True)
    if not os.path.exists(os.path.join(d, 'folders.json')):
        with open(os.path.join(d, 'folders.json'), 'w') as f:
            json.dump({'folders': {}, 'fileFolderMap': {}}, f)
    return d

def get_upload_dir(user_id):
    return os.path.join(get_user_dir(user_id), 'uploads')

def get_folders_file(user_id):
    return os.path.join(get_user_dir(user_id), 'folders.json')

def load_folders(user_id):
    try:
        with open(get_folders_file(user_id), 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return {'folders': {}, 'fileFolderMap': {}}

def save_folders(user_id, data):
    with open(get_folders_file(user_id), 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def parse_docx_text(filepath):
    try:
        with open(filepath, 'rb') as f:
            buf = f.read()
        eocd = -1
        for i in range(len(buf) - 22, max(0, len(buf) - 65557), -1):
            if buf[i:i+4] == b'PK\x05\x06':
                eocd = i
                break
        if eocd == -1:
            return ''
        cd_offset = struct.unpack_from('<I', buf, eocd + 16)[0]
        cd_count = struct.unpack_from('<H', buf, eocd + 10)[0]
        off = cd_offset
        doc_xml = None
        for _ in range(cd_count):
            fname_len = struct.unpack_from('<H', buf, off + 28)[0]
            local_off = struct.unpack_from('<I', buf, off + 42)[0]
            fname = buf[off + 46: off + 46 + fname_len].decode('utf-8', errors='replace')
            if fname == 'word/document.xml':
                comp_size = struct.unpack_from('<I', buf, local_off + 18)[0]
                name_len = struct.unpack_from('<H', buf, local_off + 26)[0]
                extra_len = struct.unpack_from('<H', buf, local_off + 28)[0]
                data_start = local_off + 30 + name_len + extra_len
                method = struct.unpack_from('<H', buf, local_off + 8)[0]
                comp_data = buf[data_start: data_start + comp_size]
                if method == 0:
                    doc_xml = comp_data.decode('utf-8', errors='replace')
                elif method == 8:
                    try:
                        doc_xml = zlib.decompressraw(comp_data).decode('utf-8', errors='replace')
                    except:
                        try:
                            doc_xml = zlib.decompress(comp_data).decode('utf-8', errors='replace')
                        except:
                            pass
                break
            off += 46 + fname_len + struct.unpack_from('<H', buf, off + 30)[0] + struct.unpack_from('<H', buf, off + 32)[0]
        if not doc_xml:
            return ''
        return ''.join(re.findall(r'<w:t[^>]*>([\s\S]*?)</w:t>', doc_xml))
    except:
        return ''

def resolve_user():
    user_id = request.cookies.get('userId')
    if not user_id:
        user_id = 'user_' + secrets.token_hex(8)
    get_user_dir(user_id)
    return user_id

@app.after_request
def set_user_cookie(response):
    user_id = resolve_user()
    if not request.cookies.get('userId'):
        response.set_cookie('userId', user_id, max_age=365*24*60*60*1000, httponly=True)
    return response

@app.route('/api/me')
def api_me():
    return jsonify({'userId': resolve_user()})

@app.route('/api/folders')
def api_get_folders():
    return jsonify(load_folders(resolve_user()))

@app.route('/api/folders', methods=['POST'])
def api_create_folder():
    data = request.get_json()
    name = (data or {}).get('name', '').strip()
    if not name:
        return jsonify({'error': 'Folder name is required'}), 400
    uid = resolve_user()
    folders = load_folders(uid)
    fid = 'folder_' + str(int(datetime.now().timestamp() * 1000))
    folders['folders'][fid] = {
        'id': fid, 'name': name,
        'parentId': (data or {}).get('parentId') or None,
        'createdAt': datetime.now().isoformat()
    }
    save_folders(uid, folders)
    return jsonify(folders['folders'][fid])

@app.route('/api/folders/<fid>', methods=['PUT'])
def api_rename_folder(fid):
    data = request.get_json()
    name = (data or {}).get('name', '').strip()
    if not name:
        return jsonify({'error': 'Folder name is required'}), 400
    uid = resolve_user()
    folders = load_folders(uid)
    if fid not in folders['folders']:
        return jsonify({'error': 'Folder not found'}), 404
    folders['folders'][fid]['name'] = name
    save_folders(uid, folders)
    return jsonify(folders['folders'][fid])

@app.route('/api/folders/<fid>', methods=['DELETE'])
def api_delete_folder(fid):
    uid = resolve_user()
    folders = load_folders(uid)
    if fid not in folders['folders']:
        return jsonify({'error': 'Folder not found'}), 404
    to_delete = set()
    def collect(f):
        to_delete.add(f)
        for k, v in folders['folders'].items():
            if v.get('parentId') == f:
                collect(k)
    collect(fid)
    for fdel in to_delete:
        for file_id in list(folders['fileFolderMap'].keys()):
            if folders['fileFolderMap'][file_id] in to_delete:
                fp = os.path.join(get_upload_dir(uid), file_id)
                if os.path.exists(fp):
                    os.remove(fp)
                del folders['fileFolderMap'][file_id]
        folders['folders'].pop(fdel, None)
    save_folders(uid, folders)
    return jsonify({'success': True})

@app.route('/api/files/<file_id>/move', methods=['POST'])
def api_move_file(file_id):
    data = request.get_json()
    uid = resolve_user()
    folders = load_folders(uid)
    folder_id = data.get('folderId') or None
    if folder_id and folder_id not in folders['folders']:
        return jsonify({'error': 'Folder not found'}), 404
    folders['fileFolderMap'][file_id] = folder_id
    save_folders(uid, folders)
    return jsonify({'success': True})

@app.route('/api/files')
def api_get_files():
    uid = resolve_user()
    folders = load_folders(uid)
    upload_dir = get_upload_dir(uid)
    files = []
    for fname in os.listdir(upload_dir):
        fp = os.path.join(upload_dir, fname)
        st = os.stat(fp)
        orig = re.sub(r'^\d+_(.+)$', r'\1', fname) or fname
        files.append({
            'id': fname, 'name': orig,
            'size': st.st_size, 'date': datetime.fromtimestamp(st.st_mtime).isoformat(),
            'folderId': folders['fileFolderMap'].get(fname) or None
        })
    return jsonify({'files': files, 'folders': folders['folders']})

@app.route('/api/upload', methods=['POST'])
def api_upload():
    if 'file' not in request.files:
        return jsonify({'error': 'No file'}), 400
    f = request.files['file']
    ext = os.path.splitext(f.filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({'error': 'Unsupported format'}), 400
    uid = resolve_user()
    safe = f.filename
    fname = str(int(datetime.now().timestamp() * 1000)) + '_' + safe
    fp = os.path.join(get_upload_dir(uid), fname)
    f.save(fp)
    return jsonify({'id': fname, 'name': safe, 'folderId': None})

@app.route('/api/files/<file_id>', methods=['DELETE'])
def api_delete_file(file_id):
    uid = resolve_user()
    fp = os.path.join(get_upload_dir(uid), file_id)
    if not os.path.exists(fp):
        return jsonify({'error': 'File not found'}), 404
    os.remove(fp)
    folders = load_folders(uid)
    folders['fileFolderMap'].pop(file_id, None)
    save_folders(uid, folders)
    return jsonify({'success': True})

@app.route('/api/files/<file_id>/download')
def api_download(file_id):
    uid = resolve_user()
    fp = os.path.join(get_upload_dir(uid), file_id)
    if not os.path.exists(fp):
        return jsonify({'error': 'File not found'}), 404
    orig = re.sub(r'^\d+_(.+)$', r'\1', file_id) or file_id
    return send_file(fp, as_attachment=True, download_name=orig)

@app.route('/api/search', methods=['POST'])
def api_search():
    data = request.get_json() or {}
    query = data.get('query', '')
    file_ids = data.get('fileIds', [])
    if not query or not file_ids:
        return jsonify([])
    uid = resolve_user()
    results = []
    for fid in file_ids:
        fp = os.path.join(get_upload_dir(uid), fid)
        if not os.path.exists(fp):
            continue
        try:
            if fid.lower().endswith('.docx'):
                content = parse_docx_text(fp)
            else:
                with open(fp, 'r', encoding='utf-8', errors='replace') as f:
                    content = f.read()
            lines = content.split('\n')
            matches = []
            for idx, line in enumerate(lines):
                if query.lower() in line.lower():
                    matches.append({'line': idx + 1, 'text': line.strip(), 'col': line.lower().index(query.lower())})
            if matches:
                orig = re.sub(r'^\d+_(.+)$', r'\1', fid) or fid
                results.append({'id': fid, 'name': orig, 'matches': matches})
        except:
            pass
    return jsonify(results)

@app.route('/api/files/<file_id>/convert', methods=['POST'])
def api_convert(file_id):
    data = request.get_json() or {}
    target = data.get('targetExt', '')
    if not target:
        return jsonify({'error': 'Target extension required'}), 400
    ext = target.lower()
    if not ext.startswith('.'):
        ext = '.' + ext
    if ext not in ALLOWED_EXTENSIONS:
        return jsonify({'error': 'Unsupported format'}), 400
    uid = resolve_user()
    fp = os.path.join(get_upload_dir(uid), file_id)
    if not os.path.exists(fp):
        return jsonify({'error': 'File not found'}), 404
    try:
        with open(fp, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
        out = content
        if ext == '.json':
            try:
                out = json.dumps(json.loads(content), indent=2, ensure_ascii=False)
            except:
                out = json.dumps({'content': content}, indent=2, ensure_ascii=False)
        elif ext == '.xml':
            if not content.strip().startswith('<?xml') and not content.strip().startswith('<'):
                lines = content.split('\n')
                escaped = [l.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;') for l in lines]
                out = '<?xml version="1.0" encoding="UTF-8"?>\n<document>\n  <content>\n' + \
                      '\n'.join(f'    <line>{l}</line>' for l in escaped) + \
                      '\n  </content>\n</document>'
        elif ext == '.html':
            if '<html' not in content.lower() and '<!doctype' not in content.lower():
                esc = content.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
                out = f'<!DOCTYPE html>\n<html lang="en">\n<head><meta charset="UTF-8"><title>Converted</title></head>\n<body>\n<pre>{esc}</pre>\n</body>\n</html>'
        elif ext == '.csv' and ',' not in content:
            out = 'text\n' + '\n'.join('"' + l.replace('"', '""') + '"' for l in content.split('\n'))
        orig = re.sub(r'^\d+_(.+)$', r'\1', file_id) or file_id
        base = os.path.splitext(orig)[0]
        new_name = str(int(datetime.now().timestamp() * 1000)) + '_' + base + ext
        new_fp = os.path.join(get_upload_dir(uid), new_name)
        with open(new_fp, 'w', encoding='utf-8') as f:
            f.write(out)
        folders = load_folders(uid)
        new_fid = folders['fileFolderMap'].get(file_id) or None
        folders['fileFolderMap'][new_name] = new_fid
        save_folders(uid, folders)
        return jsonify({'id': new_name, 'name': base + ext, 'folderId': new_fid})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def index():
    return app.send_static_file('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000, debug=True)
