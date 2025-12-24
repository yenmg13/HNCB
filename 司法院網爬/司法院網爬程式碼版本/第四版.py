# -*- coding: utf-8 -*-
"""
fj_judgment_crawler_colab.py
司法院判決書查詢自動爬蟲（Colab 版, 支援分頁與PDF/PNG快照，並產生明細頁分享URL匯總）
- 適用 Google Colab，所有輸出儲存至 /content
- 自動安裝 Playwright 與 Chromium
"""

# ====== Colab 環境初始化（穩定版）======
import sys, os, asyncio, pandas as pd, time, traceback, re, random, csv, json
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from datetime import datetime

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except:
    ZoneInfo = None

# ---- 安裝中文字型（防止中文方格）----
!apt-get install -y fonts-noto-cjk > /dev/null

# ---- 安裝必要套件（包含 Playwright 與瀏覽器依賴）----
!pip install -U playwright beautifulsoup4 > /dev/null
!playwright install --with-deps chromium

# ---- 建立 Colab 專用儲存路徑 ----
SAVE_DIR = "/content"
os.makedirs(SAVE_DIR, exist_ok=True)

# ---- 匯入 Playwright 模組 ----
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

# ====== 常數 ======
BASE_URL = "https://judgment.judicial.gov.tw/FJUD/Default_AD.aspx"

WAIT_BEFORE_CHECKBOX = (0.5, 1.2)
WAIT_AFTER_FORM_INPUT = 1
WAIT_AFTER_CLICK_QUERY = (1, 2)
WAIT_HUMAN_NOISE = (0.5, 1.5)
WAIT_DETAIL_CONTENT = (10, 15)
WAIT_PAGE_JUMP = (3, 6)
WAIT_BUSY_PAGE = 300
WAIT_AFTER_ERROR = 3

# 選擇需要的檔案類型
OUTPUT_OPTIONS = { "FULL_JSONL": True ,
           "SPLIT_JSONL": True ,
           "PDF": True ,
           "DB_CSV": True ,
           "MGMT_CSV": True ,
           "STRUCTURED_CSV": True }

PARTY_ROLE_MARKERS = ["聲請人","法定代理人","相對人","代理人","上訴人","抗告人",
            "選任辯護人","再抗告人","訴訟代理人","原告","被告","債務人",
            "債權人","參加人"]

judgment_start_markers = ["上","上列","主文", "理由", "裁定如下", "判決如下","一、","二、",
              "以上正本", "說明", "歷審裁判", "案件目前", "相關法條"
]

# L1（主文、事實、理由）支援內含空白
L1_PATTERN = r'(主\s*文|事\s*實|理\s*由)'
# L2（壹貳參…）
L2_PATTERN = r'(壹|貳|參|肆|伍|陸|柒|捌|玖|拾)、'
# L3（一二三…）
L3_PATTERN = r'(一|二|三|四|五|六|七|八|九|十)、'

HEADING_PATTERN = re.compile(
    rf'^\s*({L1_PATTERN}|{L2_PATTERN}|{L3_PATTERN})\s*$',
    re.MULTILINE
)

# 落款（中間可能有全形或半形空白）
ENDING_PATTERN = re.compile(
    r'中\s*華\s*民\s*國|書記官|法官|司法事務官'
)


# 測試模式上限（正式跑請設大或移除）
TEST_MODE_LIMIT_PAGE = 3
TEST_MODE_LIMIT_PER_PAGE = 2

BUSY_MSGS = ["系統忙碌中","操作過於頻繁", "請按「重新整理」", "請稍候再試", "系統忙碌中，請按「重新整理」。"]

PAGE_URL_LIST_PATH = os.path.join(SAVE_DIR, "Page_URL_List.txt")
CSV_RESULT_PATH = os.path.join(SAVE_DIR, "judgments.csv")
QUERY_LOG_PATH = os.path.join(SAVE_DIR, "Query_Log.txt")  # 各參數組的彙總紀錄
PDF_SNAPSHOT_PATH = os.path.join(SAVE_DIR, "PDF_Snapshots")
os.makedirs(PDF_SNAPSHOT_PATH, exist_ok=True)

# ====== 四種輸出路徑（新增） ======
JSONL_FULL_PATH = os.path.join(SAVE_DIR, "judgments_full.jsonl")
JSONL_SPLIT_PATH = os.path.join(SAVE_DIR, "judgments_split.jsonl")                 # 全文 JSONL（逐筆追加）
MGMT_CSV_PATH = os.path.join(SAVE_DIR, "mgmt_requirements.csv")            # 業管需求檔（結構化摘要）
DB_CSV_PATH   = os.path.join(SAVE_DIR, "db_records.csv")               # 資料庫檔（正規化前的扁平表）

# ====== 統一流程化 LOG（新增） ======
def log_stage(tag: str, msg: str, **kv):
    tail = " ".join([f"{k}={v}" for k, v in kv.items()]) if kv else ""
    print(f"LOG-{tag} | {msg}" + (f" | {tail}" if tail else ""))

# ====== 小工具 ======
def print_debug(msg):
    print("[DEBUG]", msg)

# 將文字轉成安全檔名
def slugify(value: str) -> str:
    banned = '\\/:*?"<>|'
    for ch in banned:
        value = value.replace(ch, "_")
    value = "_".join(value.split())           # 空白→底線
    value = re.sub(r"_+", "_", value).strip("_")
    return value[:80]                          # 避免過長

# 若檔名已存在，就自動加 _2, _3... 避免覆蓋
def uniquify(path: str) -> str:
    base, ext = os.path.splitext(path)
    i, new = 2, path
    while os.path.exists(new):
        new = f"{base}_{i}{ext}"
        i += 1
    return new

# 建立「本次參數專屬的 PDF 目錄」
def build_params_pdf_dir(params: dict) -> str:
    # 把常用條件組成可讀片段（太長會被 slugify 截斷）
    date_range = f"{params.get('start_year','')}{params.get('start_month','')}{params.get('start_day','')}" \
                 f"-{params.get('end_year','')}{params.get('end_month','')}{params.get('end_day','')}"
    parts = [
        f"date-{date_range}",
        f"year-{params.get('case_year','')}" if params.get('case_year') else "",
        f"type-{params.get('case_type','')}" if params.get('case_type') else "",
        f"no-{params.get('case_number_start','')}_{params.get('case_number_end','')}" if (params.get('case_number_start') or params.get('case_number_end')) else "",
        f"cause-{params.get('case_cause','')}" if params.get('case_cause') else "",
        f"kw-{params.get('full_text','')}" if params.get('full_text') else "",
    ]
    human = slugify("__".join([p for p in parts if p]) or "params")
    # 用台北時間加個時間戳，讓每次執行不互相覆蓋（也更好追蹤）
    ts = datetime.now(ZoneInfo("Asia/Taipei")).strftime("%Y%m%d_%H%M%S") if ZoneInfo else datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    folder = os.path.join(PDF_SNAPSHOT_PATH, f"{ts}__{human}")
    os.makedirs(folder, exist_ok=True)
    return folder

def now_tpe_str():
    if ZoneInfo:
        t = datetime.now(ZoneInfo("Asia/Taipei"))
    else:
        t = datetime.utcnow()
    return t.strftime("%Y-%m-%d %H:%M:%S")

def ensure_headers():
    # 只在檔案不存在時補上表頭
    if not os.path.exists(PAGE_URL_LIST_PATH):
        with open(PAGE_URL_LIST_PATH, 'w', encoding='utf-8') as f:
            f.write('標題\t分享網址\n')
    if not os.path.exists(QUERY_LOG_PATH):
        with open(QUERY_LOG_PATH, 'w', encoding='utf-8') as f:
            f.write('全文內容\t裁判案由\t查詢時間(台北)\t總筆數\n')

def append_query_log(full_text, case_cause, tpe_time_str, total_count):
    with open(QUERY_LOG_PATH, 'a', encoding='utf-8') as f:
        f.write(f"{full_text}\t{case_cause}\t{tpe_time_str}\t{total_count}\n")

def sanitize_filename(name):
    return re.sub(r'[\\/:*?"<>|]+', '_', name)

def clean_url(url):
    # 移除網址裡的 &ot=in 或 ot=in
    if not url:
        return ""
    parts = urlparse(url)
    qs = parse_qs(parts.query)
    qs.pop('ot', None)
    qs_str = urlencode(qs, doseq=True)
    new_parts = parts._replace(query=qs_str)
    clean = urlunparse(new_parts)
    if clean.endswith('?'):
        clean = clean[:-1]
    return clean

def save_share_url(title, url):
    # 單筆即時寫入（不再每組清空）
    with open(PAGE_URL_LIST_PATH, 'a', encoding='utf-8') as f:
        f.write(f'{title}\t{url}\n')

def normalize_title(raw_title):
    t = raw_title.strip()
    t = re.sub(r'\s+', '', t)
    return t  # 保留：主文 / 事實 / 壹、 / 一、


def extract_sections_dict(full_text: str):
    """
    解析結構 → 直接做成 dict：{ "主文": "...", "事實": "...", "一、": "...", ... }
    """
    matches = list(HEADING_PATTERN.finditer(full_text))
    result = {}

    if len(matches) == 0:
        # 沒任何結構 → 整段給 default key
        end = ENDING_PATTERN.search(full_text)
        cleaned = full_text if not end else full_text[:end.start()]
        result["全文"] = cleaned.strip()
        return result

    for idx, m in enumerate(matches):
        raw_title = m.group(0)
        title = normalize_title(raw_title)

        start = m.end()
        end = matches[idx+1].start() if idx+1 < len(matches) else len(full_text)
        block = full_text[start:end]

        # 最末段 → 用落款切掉
        if idx == len(matches) - 1:
            end2 = ENDING_PATTERN.search(block)
            if end2:
                block = block[:end2.start()]

        block = block.strip()

        # ⚠ 最重要：內容不再含標題 → 因為 regex 已切掉
        result[title] = block

    return result


def build_structured_json(full_text: str):
    """
    最終版本：輸出真正的結構化欄位 JSON 格式
    """
    sections = extract_sections_dict(full_text)
    return {
        "full_text": full_text,
        "sections": sections   # <-- dict，而不是 list
    }
# ====== 防爬/人類化操作 ======
async def is_busy_page(page):
    html = await page.content()
    return any(msg in html for msg in BUSY_MSGS)

async def human_noise(page):
    x = random.randint(50, 600)
    y = random.randint(200, 700)
    await page.mouse.move(x, y)
    if random.random() > 0.5:
        await page.mouse.wheel(0, random.randint(200, 1200))
    if random.random() > 0.7:
        await page.mouse.click(random.randint(100, 800), random.randint(200, 800))
    await asyncio.sleep(random.uniform(*WAIT_HUMAN_NOISE))

# ====== 快照 ======
async def save_pdf_snapshot(page, filename):
    await page.pdf(path=filename, format="A4", print_background=True)

async def save_png_snapshot(page, filename):
    await page.screenshot(path=filename, full_page=True)

# ====== 站內轉存PDF（優先） ======
async def download_via_site_pdf(popup, filename):
    """
    優先嘗試點擊「轉存PDF」或相近文案的按鈕；成功則以 expect_download 下載保存。
    找不到或失敗就丟出例外，呼叫端再 fallback 用 page.pdf。
    """
    # 可能需要先展開選單
    for menu_sel in ["#moreActions", "button:has-text('更多')", "text=更多功能"]:
        menu = popup.locator(menu_sel)
        if await menu.count() > 0:
            try:
                await menu.first.click()
                await asyncio.sleep(0.3)
            except:
                pass

    candidates = [
        "text=轉存PDF", "text=匯出PDF", "text=另存為PDF",
        "a:has-text('PDF')", "button:has-text('PDF')",
        "[aria-label*='PDF']",
        "#hlExportPDF", "a[href*='.pdf']", "a[href*='toPDF']"
    ]
    target = None
    for sel in candidates:
        loc = popup.locator(sel)
        if await loc.count() > 0:
            target = loc.first
            break
    if not target:
        raise RuntimeError("未找到站內『轉存/匯出PDF』按鈕")

    async with popup.expect_download() as dl_info:
        await target.click()
    download = await dl_info.value
    await download.save_as(filename)
    return filename

# ====== iframe 與列表 ======
async def get_iframe(page):
    """
    用途：在外層 Page 中尋找並回傳「查詢結果清單」所使用的 iframe 物件。
    判斷方式：抓到 <iframe id="iframe-data">，其 src 需包含 'qryresultlst.aspx'（清單頁）。
    回傳：Playwright Frame；若在約 30 秒內找不到則回傳 None。
    """
    for _ in range(60):
        iframe_elem = await page.query_selector("#iframe-data")
        if iframe_elem:
            src = await iframe_elem.get_attribute("src")
            print_debug(f"iframe src: {src}")
            if src and "qryresultlst.aspx" in src:
                for f in page.frames:
                    if src in f.url or f.name == "iframe-data":
                        print_debug(f"已正確取得 iframe，URL: {f.url}")
                        return f
        await asyncio.sleep(0.5)
    return None

async def get_result_table(iframe):
    """
    用途：在結果 iframe 中尋找清單表格 <table id="jud" class="jub-table"> 並回傳元素控制柄。
    回傳：ElementHandle 或 None（在等待期限內找不到時）。
    說明：以 0.5 秒輪詢一次、最多約 30 秒，並容忍 iframe 在載入過程中短暫失效。
    """
    for _ in range(60):  # 最多等 30 秒
        try:
            table = await iframe.query_selector('table#jud.jub-table')
            if table:
                return table
        except:
            pass
        await asyncio.sleep(0.5)
    return None

async def iframe_has_no_data_by_header_or_table(iframe):
    """
    傳統快速判斷：看 <h3>查無資料</h3> 或沒有明細連結。
    目前主流程改用 wait_results_state，這個函式保留備用。
    """
    try:
        hdr = await iframe.query_selector(".page-header h3")
        if hdr:
            text = (await hdr.inner_text() or "").replace("\u00a0", " ").strip()
            if "查無資料" in text:
                return True
        table = await get_result_table(iframe)
        if not table:
            return True
        rows = await table.query_selector_all("tbody > tr")
        for r in rows:
            link = await r.query_selector("td a[href*='id='], td a[href*='ty=JD'], td a[href*='ty=JUDBOOK']")
            if link:
                return False
        return True
    except:
        return True

async def wait_results_state(iframe, max_wait_sec=20):
    """
    等待列表結果「就緒」，回傳：
      - "HAS_DATA"  : 找到可點擊的明細列
      - "NO_DATA"   : 明確顯示查無資料（或沒有表格/沒有可點列）
      - "UNKNOWN"   : 超時仍無法判斷
    """
    steps = int(max_wait_sec / 0.5)
    for _ in range(steps):
        try:
            # A) 看是否直接顯示「查無資料」
            hdr = await iframe.query_selector(".page-header h3")
            if hdr:
                text = (await hdr.inner_text() or "").replace("\u00a0", " ").strip()
                if "查無資料" in text:
                    return "NO_DATA"

            # B) 看是否已有可點的明細列
            table = await iframe.query_selector('table#jud.jub-table')
            if table:
                rows = await table.query_selector_all("tbody > tr")
                for r in rows:
                    link = await r.query_selector("td a[href*='id='], td a[href*='ty=JD'], td a[href*='ty=JUDBOOK']")
                    if link:
                        return "HAS_DATA"
        except:
            pass
        await asyncio.sleep(0.5)

    # 超時後保險判斷
    try:
        table = await iframe.query_selector('table#jud.jub-table')
        if not table:
            return "NO_DATA"
        rows = await table.query_selector_all("tbody > tr")
        for r in rows:
            link = await r.query_selector("td a[href*='id='], td a[href*='ty=JD'], td a[href*='ty=JUDBOOK']")
            if link:
                return "HAS_DATA"
        return "NO_DATA"
    except:
        return "UNKNOWN"

async def get_total_result_count_hint(iframe):
    """
    用途：從結果頁面文字中嘗試解析「共 X 筆」的官方總筆數提示。
    回傳：int（總筆數）或 None（頁面未顯示或無法解析）。
    備註：站方在結果數 ≤ 20 筆時通常不顯示總數，因此常會回傳 None。
    """
    html = await iframe.content()
    html = html.replace('\u00a0', ' ') # 將nbsp換成一般空白
    m = re.search(r"共\s*([0-9,]+)\s*筆", html)
    if m:
        return int(m.group(1).replace(",", ""))
    return None

async def count_rows_on_current_page(iframe):
    """
    用途：計算「目前這一頁清單」的有效筆數。
    作法：抓結果表格 → 逐列檢查是否含有連到詳情頁的超連結（id= / ty=JD / ty=JUDBOOK），有就算 1 筆。
    回傳：int（該頁的有效列數）；若找不到表格則回傳 0。
    """
    table = await get_result_table(iframe)
    if not table:
        return 0
    rows = await table.query_selector_all("tbody > tr")
    cnt = 0
    for r in rows:
        link = await r.query_selector("td a[href*='id='], td a[href*='ty=JD'], td a[href*='ty=JUDBOOK']")
        if link:
            cnt += 1
    return cnt

async def get_total_pages(iframe):
    """
    用途：由分頁下拉選單（#ddlPage）估算總頁數。
    回傳：int（至少為 1）；若沒有分頁選單或沒有選項則視為 1。
    """
    select = await iframe.query_selector('select#ddlPage')
    if not select:
        return 1
    options = await select.query_selector_all('option')
    return len(options) if options else 1

async def select_page_by_index(iframe, page, target_index):
    """
    用途：在「查詢結果清單頁」中，透過下拉分頁選單(#ddlPage)切換到指定頁碼。
    備註：target_index 從 1 開始；成功觸發切頁回傳 True，否則 False。
    """
    select = await iframe.query_selector('select#ddlPage')
    if not select:
        return False

    options = await select.query_selector_all('option')
    if not options or target_index < 1 or target_index > len(options):
        return False

    opt_val = await options[target_index - 1].get_attribute('value')
    if not opt_val:
        return False
    await select.select_option(value=opt_val)
    try:
        await iframe.evaluate("""(sel)=>{ sel.dispatchEvent(new Event('change', {bubbles:true})); }""", select)
    except:
        pass
    await asyncio.sleep(random.uniform(*WAIT_PAGE_JUMP))
    await page.wait_for_timeout(1000)
    return True

async def goto_page(page, iframe, cur_page):
    try:
        return await select_page_by_index(iframe, page, cur_page)
    except Exception as e:
        print_debug(f"!!切換分頁失敗: {e}")
        return False

# ====== 查無資料（Q003）偵測 & 送出查詢 ======
async def is_no_data_q003(page):
    """
    偵測 iframe src 為 ErrorPage.aspx?err=Q003，或主頁標題顯示『查無資料』。
    """
    iframe_elem = await page.query_selector("#iframe-data")
    if iframe_elem:
        src = (await iframe_elem.get_attribute("src")) or ""
        if "ErrorPage.aspx" in src and "err=Q003" in src:
            return True

    hdr = await page.query_selector(".page-header h3")
    if hdr:
        text = (await hdr.inner_text() or "").replace("\u00a0", " ").strip()
        if "查無資料" in text:
            return True
    return False

async def submit_and_get_iframe(page):
    """
    送出查詢並嘗試取得 iframe 結果頁。
    遇 ErrorPage.aspx?err=Q003 或頁標『查無資料』→ (None, 'NO_DATA')
    正常取得列表 iframe → (iframe, 'OK')
    其他失敗 → (None, 'UNKNOWN')
    """
    print_debug("送出查詢條件...")
    await page.click("#btnQry")
    await asyncio.sleep(random.uniform(*WAIT_AFTER_CLICK_QUERY))

    for _ in range(60):  # 最多等 30 秒
        # 先判斷是否『查無資料』
        if await is_no_data_q003(page):
            print_debug("🔍 查無資料（ErrorPage Q003 / 頁標），結束本參數組。")
            return None, "NO_DATA"

        # 嘗試取得結果 iframe
        iframe_elem = await page.query_selector("#iframe-data")
        if iframe_elem:
            src = await iframe_elem.get_attribute("src")
            if src:
                print_debug(f"iframe src: {src}")
                if "qryresultlst.aspx" in src:
                    for f in page.frames:
                        if src in f.url or f.name == "iframe-data":
                            print_debug(f"已正確取得 iframe，URL: {f.url}")
                            return f, "OK"

        await asyncio.sleep(0.5)

    print_debug("未正確取得 iframe（非 Q003），流程終止")
    return None, "UNKNOWN"

# ====== 參數載入與 CSV 寫入 ======
def load_parameters_from_csv(filepath):
    df = pd.read_csv(filepath, dtype=str).fillna("")
    params_list = []
    for _, row in df.iterrows():
        params_list.append({
            "case_year": row["case_year"].strip(),
            "case_type": row["case_type"].strip(),
            "case_number_start": row["case_number_start"].strip(),
            "case_number_end": row["case_number_end"].strip(),
            "case_cause": row["case_cause"].strip(),
            "case_judgement": row["case_judgement"].strip(),
            "full_text": row["full_text"].strip(),
            "case_size_min": row["case_size_min"].strip(),
            "case_size_max": row["case_size_max"].strip(),
            "start_year": row["start_year"].strip(),
            "start_month": row["start_month"].zfill(2),
            "start_day": row["start_day"].zfill(2),
            "end_year": row["end_year"].strip(),
            "end_month": row["end_month"].zfill(2),
            "end_day": row["end_day"].zfill(2),
            "case_category": [x.strip() for x in row["case_category"].split(';') if x.strip()]
        })
    return params_list

def save_structured_result(data_dict):
    file_exists = os.path.exists(CSV_RESULT_PATH)
    with open(CSV_RESULT_PATH, 'a', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=list(data_dict.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(data_dict)

# ====== 四種輸出 I/O（新增） ======
def _ensure_csv_headers(path: str, headers: list):
    need_header = not os.path.exists(path)
    f = open(path, "a", newline="", encoding="utf-8-sig")
    w = csv.DictWriter(f, fieldnames=headers)
    if need_header:
        w.writeheader()
    # 回傳 writer 與檔案 handle（由呼叫端關閉，避免頻繁開關檔）
    return w, f

def append_split_jsonl(row: dict):
    with open(JSONL_SPLIT_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

def append_full_jsonl(row: dict):
    with open(JSONL_FULL_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")

def append_mgmt_csv(row: dict):
    headers = ["url","court","year","zi","number","case_type","judgment_type","cause","has_pdf","fetched_at"]
    w, fh = _ensure_csv_headers(MGMT_CSV_PATH, headers)
    try:
        w.writerow({k: row.get(k, "") for k in headers})
    finally:
        fh.close()

def append_db_csv(row: dict):
    headers = ["jud_key","url","court","year","zi","number","case_type","judgment_type","pdf_path","created_at"]
    w, fh = _ensure_csv_headers(DB_CSV_PATH, headers)
    try:
        w.writerow({k: row.get(k, "") for k in headers})
    finally:
        fh.close()

def get_first_div_by_id(soup):
    """
    用 div id (pasted_paragraph_) 鎖定第一個段落
    - 不排序，只取 HTML 原始順序的第一個匹配
    - 返回 (id, 文字)
    """
    first_div = soup.find("div", id=re.compile(r"^pasted_paragraph_"))
    if not first_div:
        return None, None

    # 取文字（修復 abbr 拆字與空白）
    html = str(first_div)
    html = re.sub(r'<abbr[^>]*>([^<]*)</abbr>\s*人', r'\1人', html)
    html = re.sub(r'<abbr[^>]*>([^<]*)</abbr>', r'\1', html)
    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
    text = re.sub(r"\s+", " ", text).strip()
    # 去除中文字間多餘空白
    text = re.sub(r"(?<=[\u4e00-\u9fa5])\s+(?=[\u4e00-\u9fa5])", "", text)
    return first_div["id"], text


def clean_html_before_extract(soup):
    """移除頁尾垃圾與非正文區塊"""
    junk_selectors = [
        "#qouteurl", ".btn", ".related", ".footer",
        ".lawlist", ".noprint", ".share", ".function",
        ".nav", ".breadcrumb", ".header", ".announce"
    ]
    for sel in junk_selectors:
        for junk in soup.select(sel):
            junk.decompose()
    return soup

# ====== 填表 ======
async def fill_query_form(page, params):
    print_debug("載入查詢頁面...")
    await page.goto(BASE_URL, timeout=60000)
    await asyncio.sleep(2)

    print_debug("勾選案件類別...")
    for v in params["case_category"]:
        cb_selector = f'input[name="jud_sys"][value="{v}"]'
        await page.check(cb_selector)
        await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))

    print_debug("輸入裁判期間...")
    await page.fill("#dy1", params["start_year"])
    await page.fill("#dm1", params["start_month"])
    await page.fill("#dd1", params["start_day"])
    await page.fill("#dy2", params["end_year"])
    await page.fill("#dm2", params["end_month"])
    await page.fill("#dd2", params["end_day"])
    await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))

    print_debug("輸入裁判字號與關鍵字...")
    if params["case_year"]:
        await page.fill("#jud_year", params["case_year"]); await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))
    if params["case_type"]:
        await page.fill("#jud_case", params["case_type"]); await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))
    if params["case_number_start"]:
        await page.fill("#jud_no", params["case_number_start"]); await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))
    if params["case_cause"]:
        await page.fill("#jud_title", params["case_cause"]); await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))
    if params["case_judgement"]:
        await page.fill("#jud_jmain", params["case_judgement"]); await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))
    if params["full_text"]:
        await page.fill("#jud_kw", params["full_text"]); await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))
    if params["case_size_min"]:
        await page.fill("#KbStart", params["case_size_min"]); await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))
    if params["case_size_max"]:
        await page.fill("#KbEnd", params["case_size_max"]); await asyncio.sleep(random.uniform(*WAIT_BEFORE_CHECKBOX))

    await asyncio.sleep(WAIT_AFTER_FORM_INPUT)

# ====== 大函式：提交查詢並取得結果 iframe（新增） ======
async def submit_query_and_get_iframe(page, params):
    await fill_query_form(page, params)
    iframe, status = await submit_and_get_iframe(page)

    if status == "NO_DATA":
        # 正常情況：參數組合真的沒有資料，不記 log
        return None, "NO_DATA"

    if iframe is None:
        # 技術性問題：必須記 LOG-IFRAME
        log_stage("IFRAME", "此參數組合無法取得結果 iframe（非Q003）", params=str(params)[:120])
        return None, "UNKNOWN"

    # 等結果就緒
    state = await wait_results_state(iframe, max_wait_sec=20)
    if state == "NO_DATA":
        return None, "NO_DATA"
    if state == "UNKNOWN":
        # DOM 變動/逾時，當成 0 筆回報（不當錯）
        return None, "UNKNOWN"

    return iframe, "OK"

# ====== 分頁大函式（取代原 process_search_pages） ======
async def process_result_pages(page, iframe, context, params, params_pdf_dir=None):
    # —— 第一階段：成功進入分層頁（分層頁 = 結果清單頁）——
    log_stage("STAGE1", "已進入結果清單頁（分層頁）")

    # 先嘗試「共 X 筆」
    total_count_hint = await get_total_result_count_hint(iframe)
    final_total_count = None
    if total_count_hint is not None:
        final_total_count = total_count_hint
    else:
        # 估算：單頁/最後一頁回推
        try:
            total_pages = await get_total_pages(iframe)
            if total_pages <= 1:
                first_page_rows = await count_rows_on_current_page(iframe)
                final_total_count = first_page_rows
            else:
                # 跳最後一頁數列
                cur_iframe = iframe
                await select_page_by_index(iframe, page, total_pages)
                await asyncio.sleep(1.0)
                cur_iframe = await get_iframe(page)
                last_rows = await count_rows_on_current_page(cur_iframe)

                # 回第一頁拿 page_size
                await select_page_by_index(cur_iframe, page, 1)
                await asyncio.sleep(1.0)
                cur_iframe = await get_iframe(page)
                page_size = await count_rows_on_current_page(cur_iframe) or 20
                final_total_count = (total_pages - 1) * page_size + last_rows

                # 回第一頁開始處理
                await select_page_by_index(cur_iframe, page, 1)
                await asyncio.sleep(0.8)
                iframe = await get_iframe(page)
        except Exception as e:
            log_stage("STAGE1", "無法計算總筆數（將仍嘗試抓取本頁）", error=str(e))

    processed_count = 0
    max_page = TEST_MODE_LIMIT_PAGE  # 你的測試上限

    for cur_page in range(1, max_page + 1):
        print_debug(f"==開始處理第 {cur_page} 頁==")

        # 翻頁（第1頁不用）
        if cur_page > 1:
            ok = await goto_page(page, iframe, cur_page)
            if not ok:
                break
            await asyncio.sleep(1.5)
            iframe = await get_iframe(page)
            if not iframe or "qryresultlst.aspx" not in (iframe.url or ""):
                print_debug("頁面跳轉後找不到有效 iframe，停止。")
                break
            # —— 第三階段 LOG：翻頁成功 ——
            log_stage("STAGE3", "前往下一分頁", next_page=cur_page)

        # 擷取本頁所有明細 URL（這裡直接抓 element，讓明細階段去點）
        table = await get_result_table(iframe)
        if not table:
            log_stage("URLS", "查無結果 table（本頁）")
            break

        rows = await table.query_selector_all("tbody > tr")
        url_elems = []
        for r in rows:
            link = await r.query_selector("td a[href*='id='], td a[href*='ty=JD'], td a[href*='ty=JUDBOOK']")
            if link:
                url_elems.append(link)

        if not url_elems:
            log_stage("URLS", "未擷取到任何明細 URL（請檢查 selector 或頁面變更）")
            break
        else:
            log_stage("URLS", "已擷取明細 URL", count=len(url_elems))

        # —— 明細階段（第二階段 LOG 在裡面）——
        handled, hit_limit = await process_details(page, url_elems, context, params, params_pdf_dir)
        processed_count += handled

        # 單頁上限（你的 TEST_MODE_LIMIT_PER_PAGE）
        if hit_limit:
            log_stage("STAGE3", "達到本分頁處理上限", page_index=cur_page, handled=handled)

    return processed_count, (final_total_count if final_total_count is not None else processed_count)

# ====== 明細大函式（新增） ======
async def process_details(page, url_elements, context, params, params_pdf_dir=None):
    """
    回傳：(本頁處理數, 是否達上限)
    - 逐筆開啟明細 → 擷取資料 → 產出檔案（JSONL / PDF or PNG / 業管CSV / DB CSV）
    - 產出檔案前記錄 LOG-STAGE2
    """
    base_dir = params_pdf_dir or PDF_SNAPSHOT_PATH
    os.makedirs(base_dir, exist_ok=True)

    handled = 0
    for idx, link in enumerate(url_elements, start=1):
        if idx > TEST_MODE_LIMIT_PER_PAGE:
            return handled, True

        try:
            # 嘗試用新頁開啟，容忍同頁導航
            async with context.expect_page() as popup_info:
                try:
                    await link.evaluate("a => a.target = '_blank'")
                except:
                    pass
                await link.click(button="middle")
            try:
                popup = await popup_info.value
                page_like = popup
            except:
                popup = None
                page_like = page

            await page_like.bring_to_front()
            await page_like.wait_for_load_state("domcontentloaded", timeout=20000)
            await asyncio.sleep(random.uniform(*WAIT_DETAIL_CONTENT))
            await human_noise(page_like)

            # ====== 基本命名與分享連結 ======
            share_link = clean_url(page_like.url)
            html = await page_like.content()
            soup = BeautifulSoup(html, "html.parser")

            # 取「裁判字號／裁判日期／裁判案由」
            def get_value_by_label_bs(soup, label):
                for th, td in zip(soup.select("div.col-th"), soup.select("div.col-td")):
                    th_text = th.get_text(strip=True)
                    if label in th_text:
                        return td.get_text(strip=True)
                return ""

            title_text = get_value_by_label_bs(soup, "裁判字號")
            date_text_raw = get_value_by_label_bs(soup, "裁判日期")
            cause_text = get_value_by_label_bs(soup, "裁判案由")

            # 記錄分享清單
            date_roc = date_text_raw or ""
            date_str = re.sub(r"民國\s*(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
                              lambda m: f"{int(m.group(1))}/{int(m.group(2)):02}/{int(m.group(3)):02}", date_roc)
            share_title = f"{date_str}_{(title_text or '').replace(' ', '')}"
            save_share_url(share_title, share_link or "")

            # 解析民國日期 → yyy/mm/dd（檔名用）
            date_text = re.sub(
                r"民國\s*(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日",
                lambda m: f"{int(m.group(1))}/{int(m.group(2)):02}/{int(m.group(3)):02}",
                date_text_raw or ""
            )

            # 簡化檔名（用裁判字號與日期）
            base_name = sanitize_filename((date_text or "date") + "_" + (title_text or "judgment"))
            filename_base = os.path.join(base_dir, base_name)
            pdf_path = filename_base + ".pdf"
            png_path = filename_base + ".png"

            # ====== 角色/欄位解析（沿用你原本邏輯） ======

            def extract_parties_from_abbr(soup, debug=False):
                role_set = set(PARTY_ROLE_MARKERS)
                result = {r: [] for r in PARTY_ROLE_MARKERS}

                paragraphs = soup.find_all("div", id=re.compile(r"^(pasted_|new_)?paragraph_"))
                merged_lines = []
                buffer = ""
                prev_text = ""

                for idx, div in enumerate(paragraphs):
                    # Step 1️ 修復 abbr 結構
                    html = str(div)
                    html = re.sub(r'<abbr[^>]*>([^<]*)</abbr>\s*人', r'\1人', html)
                    html = re.sub(r'<abbr[^>]*>([^<]*)</abbr>\s*律師', r'\1律師', html)
                    html = re.sub(r'<abbr[^>]*>([^<]*)</abbr>', r'\1', html)
                    html = html.replace("&nbsp;", " ")

                    # Step 2️ 轉為乾淨文字 + 修正中文字間空白
                    text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
                    text = re.sub(r"\s+", " ", text).strip()
                    text = re.sub(r"(?<=[\u4e00-\u9fa5])\s+(?=[\u4e00-\u9fa5])", "", text)
                    if not text:
                        continue

                    # Step 2.1️ 檢查是否為跨行角色（例：聲請人 / 即 債務人）
                    if idx + 1 < len(paragraphs):
                        next_html = str(paragraphs[idx + 1])
                        next_text = BeautifulSoup(next_html, "html.parser").get_text(" ", strip=True)
                        next_text = re.sub(r"\s+", " ", next_text).strip()
                        next_text = re.sub(r"(?<=[\u4e00-\u9fa5])\s+(?=[\u4e00-\u9fa5])", "", next_text)
                        if re.search(r"(人|律師)$", text) and next_text.startswith("即"):
                            text = text + " " + next_text
                            if debug:
                                print(f"  ↳ [跨行角色延伸] {text}")

                    if debug:
                        print(f"[DEBUG] 行 {idx+1}: {text}")

                    # Step 2.2 若上一行以「即」結尾且本行有角色 → 合併
                    if buffer and re.search(r"即\s*$", buffer) and any(role in text[:8] for role in role_set):
                        if debug:
                            print(f"  ↳ [MERGE 即結構] {buffer[-10:]} + {text[:15]}")
                        buffer = buffer.rstrip("即").strip() + " 即 " + text
                        prev_text = text
                        continue

                    # Step 2.3 若本行以「即」開頭（包含 abbr 結構） → 合併到上一行
                    if text.strip().startswith("即") or re.match(r"^即\s*<abbr", html):
                            # 若上一行 buffer 已經含有本行內容 → 跳過（防重複）
                        if buffer and text in buffer:
                            if debug:
                                print(f"  ↳ [跳過重複即行] {text[:20]}")
                            continue


                        # 嘗試修復 abbr 中拆開的角色詞
                        fixed_text = re.sub(r'<abbr[^>]*>([^<]*)</abbr>\s*人', r'\1人', html)
                        fixed_text = re.sub(r'<abbr[^>]*>([^<]*)</abbr>', r'\1', fixed_text)
                        fixed_text = BeautifulSoup(fixed_text, "html.parser").get_text(" ", strip=True)
                        fixed_text = re.sub(r"\s+", " ", fixed_text)
                        fixed_text = re.sub(r"(?<=[\u4e00-\u9fa5])\s+(?=[\u4e00-\u9fa5])", "", fixed_text)

                        # 若裡面有角色關鍵字 → 合併
                        if buffer:
                            if debug:
                                print(f"  ↳ [MERGE 即結構-下即上角] {buffer[-10:]} + {fixed_text[:20]}")
                            buffer = buffer.strip() + " " + fixed_text
                            prev_text = fixed_text
                            continue


                    # Step 3️ 判斷主文 or 角色行
                    if any(stop in text for stop in judgment_start_markers):
                        if buffer:
                            merged_lines.append(buffer.strip())
                            buffer = ""
                        if debug:
                            print("  ↳ [主文偵測] 偵測到正文開頭詞 → 結束角色段")
                        break

                    has_role = any(role in text for role in role_set)
                    is_new_role = False
                    is_continuation = False

                    if has_role and re.match(r"^.{0,5}(" + "|".join(role_set) + ")", text):
                        is_new_role = True

                    if not is_new_role:
                        if re.match(r"^[、,，及與或同另（(]", text):
                            is_continuation = True
                        elif re.search(r"(股份有限公司|公司|事務所|大學|醫院)$", text):
                            is_continuation = True
                        elif re.match(r"^[\u4e00-\u9fa5]{2,4}$", text):
                            is_continuation = True
                        elif not any(stop in text for stop in judgment_start_markers):
                            is_continuation = True

                    if is_new_role:
                        if buffer:
                            merged_lines.append(buffer.strip())
                        buffer = text
                        if debug:
                            print("  ↳ [新角色行] 開始新的角色段")
                    elif is_continuation:
                        buffer += " " + text
                        if debug:
                            print("  ↳ [延續行] 合併進上一角色段")
                    else:
                        if buffer:
                            merged_lines.append(buffer.strip())
                            buffer = ""
                        if debug:
                            print("  ↳ [非角色行] 結束角色段")
                    prev_text = text

                if buffer:
                    merged_lines.append(buffer.strip())

                if debug:
                    print("\n[DEBUG] 合併後角色區段：")
                    for i, seg in enumerate(merged_lines, 1):
                        print(f"  {i}. {seg}")

                # Step 5️ 角色內容解析（改良版）
                for line in merged_lines:
                    # 同行可有多角色，長詞優先
                    matched_roles = []
                    for r in sorted(role_set, key=len, reverse=True):
                        if r not in line:
                            continue
                        # 若本行同時包含「法定代理人」且 r 是「代理人」，則跳過，避免重疊
                        if r == "代理人" and any(big in line for big in ["法定代理人", "訴訟代理人", "選任代理人"]):
                            continue
                        matched_roles.append(r)
                    if not matched_roles:
                        continue

                    for matched_role in matched_roles:
                        after = line.split(matched_role, 1)[-1].strip()
                        after = re.sub(r"[\(\)（）\[\]]", "", after)

                        # 若仍含主文詞 → 截斷
                        for stop in judgment_start_markers:
                            if stop in after:
                                after = after.split(stop)[0].strip()

                        if len(after) < 1 or len(after) > 150:
                            continue

                        # 「即」邏輯：A(即B) 兩角色共用同一內容
                        m = re.search(r"即\s*([^\s,，、]{1,6}?人)", after)
                        if m:
                            alias_role = m.group(1)
                            if alias_role in role_set:
                                cleaned = re.sub(r"即\s*" + re.escape(alias_role), "", after)
                                cleaned = cleaned.strip(" ,;；、")

                                # ⚠️ 避免重複寫入
                                if cleaned and cleaned not in result[matched_role]:
                                    result[matched_role].append(cleaned)
                                if cleaned and cleaned not in result[alias_role]:
                                    result[alias_role].append(cleaned)

                                if debug:
                                    print(f"  ↳ [即結構雙角色] {matched_role}, {alias_role}: {cleaned}")
                                continue

                        after = re.sub(r"\s+", " ", after).strip(" ,;；、")
                        result[matched_role].append(after)
                        if debug:
                            print(f"  ↳ [角色抽取] {matched_role}: {after}")

                # Step 6 清理重複輸出
                merged = {}
                for role, vals in result.items():
                    seen, clean = set(), []
                    for v in vals:
                        v = re.sub(r"\s+", " ", v).strip(" ,;；、")
                        if v and v not in seen:
                            seen.add(v)
                            clean.append(v)
                    merged[role] = ", ".join(clean)

                if debug:
                    print("\n[DEBUG] 最終結構化結果：")
                    for k, v in merged.items():
                        if v:
                            print(f"  {k}: {v}")

                return merged


            # === 呼叫流程 ===
            soup = clean_html_before_extract(soup)
            first_id, first_text = get_first_div_by_id(soup)
            print("🔹第一個 div:", first_id)
            print("🔹文字內容:", first_text)
            party_data = extract_parties_from_abbr(soup, debug=True)

            # 裁判字號細拆（沿用你的 regex）
            pattern = (r'(?P<法院>.+?)\s*(?P<年度>\d{2,3})\s*年度(?P<字>\S+?)字第\s*(?P<字號>\d+)\s*號(?P<刑民>(民事|刑事))?\s*(?P<裁定類型>裁定|判決|裁判|命令|裁判|其他)?')
            match = re.match(pattern, title_text or "")
            court = match.group("法院") if match else ""
            year = match.group("年度") if match else ""
            case_word = match.group("字") if match else ""
            case_number = match.group("字號") if match else ""
            case_type = match.group("刑民") if match and match.group("刑民") else ""
            judgment_type = match.group("裁定類型") if match and match.group("裁定類型") else ""

            # 嘗試抓取「正文文字」供 JSONL（若抓不到就以摘要為主）
            body_text = ""
            main_candidates = ["#jud-content",".jud","div.card-block","div#content"]
            for sel in main_candidates:
                node = soup.select_one(sel)
                if node:
                    body_text = node.get_text("\n", strip=True)
                    break
            if not body_text:
                body_text = soup.get_text("\n", strip=True)[:15000]  # 防太長

            # ====== 產生結構化段落 ======
            structured_json = build_structured_json(body_text)

            # —— 第二階段：產出檔案前 LOG ——
            log_stage("STAGE2", "即將產出檔案（JSON/PDF/業管CSV/DBCSV）", url=share_link)

            fetched_at = now_tpe_str()

            # ====== 下載 PDF（站內轉存，失敗則打印 PDF）＋ PNG ======
            if OUTPUT_OPTIONS.get("PDF" , False):
              try:
                if not (os.path.exists(pdf_path) and os.path.exists(png_path)):
                    try:
                        await download_via_site_pdf(page_like, pdf_path)
                        print_debug(f"已用『站內轉存PDF』下載：{pdf_path}")
                    except Exception as e_download:
                        print_debug(f"站內轉存PDF失敗，改用打印PDF備援：{e_download}")
                        await save_pdf_snapshot(page_like, pdf_path)
                        print_debug(f"已存PDF(備援)：{pdf_path}")
                    await save_png_snapshot(page_like, png_path)
                    print_debug(f"已截圖：{png_path}")

              except Exception as e:
                log_stage("PDFPNG", "PDF/PNG 輸出失敗", error=str(e))
                print_debug(f"PDF/PNG 輸出失敗：{e}")

            # 1-1) FULL_JSONL（全文/摘要）
            if OUTPUT_OPTIONS.get("FULL_JSONL" , False):
              append_full_jsonl({
                  "url": share_link,
                  "body_text": body_text
              })
              print_debug(f"已儲存FULL_JSONL：{JSONL_FULL_PATH}")


            # 1-2) SPLIT_JSONL（全文/摘要）
            if OUTPUT_OPTIONS.get("SPLIT_JSONL" , False):
              append_split_jsonl({
                  "url": share_link,
                  "fetched_at": fetched_at,
                  "title": title_text,
                  "date": date_text,
                  "cause": cause_text,
                  **{r: party_data.get(r, "") for r in PARTY_ROLE_MARKERS},  # 安全展開補空
                  "court": court,
                  "year": year,
                  "zi": case_word,
                  "number": case_number,
                  "case_type": case_type,
                  "judgment_type": judgment_type,
                  "pdf_path": pdf_path,
                  "png_path": png_path,
                  "structured_content": structured_json
              })
              print_debug(f"已儲存SPLIT_JSONL：{JSONL_SPLIT_PATH}")
            # 2) 業管 CSV（精簡摘要）
            if OUTPUT_OPTIONS.get("MGMT_CSV" , False):
              append_mgmt_csv({
                  "url": share_link,
                  "court": court,
                  "year": year,
                  "zi": case_word,
                  "number": case_number,
                  "case_type": case_type,
                  "judgment_type": judgment_type,
                  "cause": cause_text,
                  "has_pdf": os.path.exists(pdf_path),
                  "fetched_at": fetched_at
              })
              print_debug(f"已儲存MGMT_CSV：{MGMT_CSV_PATH}")

            # 3) 資料庫 CSV（扁平、後續可正規化）
            if OUTPUT_OPTIONS.get("DB_CSV" , False):
              jud_key = f"{court}-{year}-{case_word}-{case_number}".strip("-")
              append_db_csv({
                  "jud_key": jud_key,
                  "url": share_link,
                  "court": court,
                  "year": year,
                  "zi": case_word,
                  "number": case_number,
                  "case_type": case_type,
                  "judgment_type": judgment_type,
                  "pdf_path": pdf_path,
                  "created_at": fetched_at
              })
              print_debug(f"已儲存DB_CSV：{DB_CSV_PATH}")

            # 4) 你原本的「結構化 CSV」（保留）
            if OUTPUT_OPTIONS.get("STRUCTURED_CSV" , False):
              result = {
                  "裁判字號": title_text,
                  "裁判字號_法院": court,
                  "裁判字號_年度": year,
                  "裁判字號_字": case_word,
                  "裁判字號_字號": case_number,
                  "裁判字號_刑事民事": case_type,
                  "裁判字號_裁定": judgment_type,
                  "裁判日期": date_text,
                  "裁判案由": cause_text,
                  **{r: party_data.get(r, "") for r in PARTY_ROLE_MARKERS},  # 安全展開補空
                  "明細網址": share_link,
              }
              save_structured_result(result)
              print_debug(f"已儲存STRUCTURED_CSV：{CSV_RESULT_PATH}")

            # 防爬暫停
            if await is_busy_page(page_like):
                print_debug("!!! 偵測到防爬蟲流量限制，暫停 5 分鐘再繼續...")
                if popup:
                    await page_like.close()
                await asyncio.sleep(WAIT_BUSY_PAGE)

            if popup:
                await page_like.close()

            handled += 1

        except Exception as e:
            # —— DETAIL 失敗回報 ——
            log_stage("DETAIL", "明細處理失敗", error=str(e))
            try:
                if 'page_like' in locals() and page_like:
                    err_base = os.path.join(base_dir, f"error_{idx}")
                    await save_png_snapshot(page_like, err_base + ".png")
                    await save_pdf_snapshot(page_like, err_base + ".pdf")
                    try:
                        await page_like.close()
                    except:
                        pass
                await asyncio.sleep(WAIT_AFTER_ERROR)
            except:
                pass
            continue

    return handled, False

# ====== 單組參數流程（整合新的區塊） ======
async def run_scraper_with_params(params):
    async with async_playwright() as p:
        # 下載一定要開 accept_downloads
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', "--disable-dev-shm-usage"])
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        # —— 瀏覽器啟動回報 ——
        log_stage("BOOT", "瀏覽器啟動成功")

        try:
            # [ADD] 為這一組參數建立專屬 PDF 目錄
            params_pdf_dir = build_params_pdf_dir(params)

            # 查詢 → 取得 iframe（含 Q003、UNKNOWN 處理）
            iframe, status = await submit_query_and_get_iframe(page, params)

            # 如果頁面直接呈現NO_DATA或是無iframe，則直接將參數組合紀錄至query_log.txt
            if status == "NO_DATA":
                append_query_log(params.get("full_text",""), params.get("case_cause",""), now_tpe_str(), 0)
                return
            if status != "OK" or not iframe:
                # IFRAME 技術性失敗已在 submit_query_and_get_iframe() 被 LOG-IFRAME 記錄
                append_query_log(params.get("full_text",""), params.get("case_cause",""), now_tpe_str(), 0)
                return

            # 分頁 + 明細（含三階段 LOG）
            processed_count, final_total_count = await process_result_pages(page, iframe, context, params, params_pdf_dir)

            # 寫入查詢彙總紀錄
            append_query_log(params.get("full_text",""), params.get("case_cause",""), now_tpe_str(), final_total_count)

        # 錯誤例外
        except PlaywrightTimeoutError as e:
            print_debug(f"Timeout發生: {e}")
            append_query_log(params.get("full_text",""), params.get("case_cause",""), now_tpe_str(), 0)
        except Exception as e:
            print_debug("主流程出現錯誤: " + traceback.format_exc())
            append_query_log(params.get("full_text",""), params.get("case_cause",""), now_tpe_str(), 0)
        finally:
            print_debug("程式結束")
            if browser:
                await browser.close()

# ====== 批次流程 ======
async def run_all_tasks():
    """
    從 CSV 檔案中讀取所有查詢參數組，依序呼叫爬蟲主流程。
    """
    # 一次性初始化（不再每組清空）
    ensure_headers()

    # 參數檔路徑更穩健：優先 params(name).csv，找不到用 params.csv
    preferred = os.path.join(SAVE_DIR, "params(name).csv")
    fallback = os.path.join(SAVE_DIR, "params.csv")
    if os.path.exists(preferred):
        csv_path = preferred
    elif os.path.exists(fallback):
        csv_path = fallback
    else:
        print("⚠️ 錯誤：未找到 params(name).csv 或 params.csv，請上傳後重新執行。")
        from google.colab import files
        files.upload()
        if os.path.exists(preferred):
            csv_path = preferred
        elif os.path.exists(fallback):
            csv_path = fallback
        else:
            print("❌ 上傳失敗或仍未找到檔案，程式終止。")
            return

    params_list = load_parameters_from_csv(csv_path)
    for i, params in enumerate(params_list, 1):
        print_debug(f"========== 開始第 {i} 組參數 ==========")
        await run_scraper_with_params(params)

# ====== 入口 ======
if __name__ == "__main__":
    await run_all_tasks()
