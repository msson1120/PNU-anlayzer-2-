import asyncio
import io
import json
import random
import re
import socket
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import pandas as pd
import streamlit as st

# =========================
# 기본 설정
# =========================
BASE_URL = "https://apis.data.go.kr/1613000/BldRgstHubService"
OPERATION = "getBrTitleInfo"
PNU_RE = re.compile(r"^\d{19}$")
DEFAULT_KEY = ""

COLUMN_RENAME_MAP = {
    "PNU": "PNU",
    "platPlc": "지번주소",
    "newPlatPlc": "도로명주소",
    "bldNm": "건물명",
    "mainPurpsCdNm": "주용도명",
    "mainPurpsCd": "주용도코드",
    "etcPurps": "기타용도",
    "dongNm": "동명",
    "mainAtchGbCdNm": "주부속구분",
    "regstrKindCdNm": "대장종류",
    "hhldCnt": "세대수",
    "grndFlrCnt": "지상층수",
    "ugrndFlrCnt": "지하층수",
    "archArea": "건축면적(㎡)",
    "totArea": "연면적(㎡)",
    "bcRat": "건폐율(%)",
    "vlRat": "용적률(%)",
    "pmsDay": "허가일",
    "useAprDay": "사용승인일",
    "crtnDay": "데이터생성일자",
    "queryAt": "조회일시",
    "strctCdNm": "구조명",
    "_status": "조회상태",
    "_memo": "오류메모",
}

OUTPUT_COLS = [
    "PNU",
    "platPlc",
    "newPlatPlc",
    "bldNm",
    "mainPurpsCdNm",
    "dongNm",
    "mainAtchGbCdNm",
    "regstrKindCdNm",
    "mainPurpsCd",
    "etcPurps",
    "hhldCnt",
    "grndFlrCnt",
    "ugrndFlrCnt",
    "archArea",
    "totArea",
    "bcRat",
    "vlRat",
    "pmsDay",
    "useAprDay",
    "crtnDay",
    "queryAt",
    "strctCdNm",
    "_status",
    "_memo",
]

SUMMARY_COLS = [
    "PNU",
    "지번주소",
    "도로명주소",
    "건물명",
    "주용도명",
    "세대수_합계",
    "건물동수",
    "최신_데이터생성일자",
    "조회일시",
]


# =========================
# 유틸
# =========================
def normalize_pnu_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.str.replace(r"\s+", "", regex=True)
    s = s.str.replace(r"\.0$", "", regex=True)

    def fix(x: str) -> str:
        if x == "" or x.lower() == "nan":
            return ""
        if "e" in x.lower():
            try:
                x = str(int(float(x)))
            except Exception:
                return x
        if x.isdigit() and len(x) < 19:
            x = x.zfill(19)
        return x

    return s.map(fix)



def pnu_to_params(pnu: str) -> Optional[Dict[str, str]]:
    pnu = (pnu or "").strip()
    if not PNU_RE.fullmatch(pnu):
        return None

    pnu_plat = pnu[10:11]
    if pnu_plat == "1":
        plat_gb = "0"  # 일반 -> 대지
    elif pnu_plat == "2":
        plat_gb = "1"  # 산 -> 산
    else:
        plat_gb = "0"

    return {
        "sigunguCd": pnu[0:5],
        "bjdongCd": pnu[5:10],
        "platGbCd": plat_gb,
        "bun": pnu[11:15],
        "ji": pnu[15:19],
    }



def extract_items(data: Any) -> Tuple[List[Dict[str, Any]], int]:
    total = 0

    def find(obj: Any, key: str) -> Any:
        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            for v in obj.values():
                r = find(v, key)
                if r is not None:
                    return r
        elif isinstance(obj, list):
            for it in obj:
                r = find(it, key)
                if r is not None:
                    return r
        return None

    tc = find(data, "totalCount")
    if tc is not None:
        try:
            total = int(str(tc).strip())
        except Exception:
            pass

    items_obj = find(data, "items")
    if isinstance(items_obj, dict):
        item = items_obj.get("item")
        if item is None:
            return [], total
        if isinstance(item, list):
            return [i for i in item if isinstance(i, dict)], total
        if isinstance(item, dict):
            return [item], total
        return [], total

    item_obj = find(data, "item")
    if isinstance(item_obj, list):
        return [i for i in item_obj if isinstance(i, dict)], total
    if isinstance(item_obj, dict):
        return [item_obj], total
    return [], total


class CancelFlag:
    def __init__(self) -> None:
        self.is_canceled = False


# =========================
# API 호출
# =========================
async def fetch_pnu(
    session: aiohttp.ClientSession,
    pnu: str,
    service_key: str,
    sem: asyncio.Semaphore,
    num_rows: int = 100,
    timeout_sec: int = 25,
    max_retries: int = 6,
    max_pages: int = 50,
) -> List[Dict[str, Any]]:
    params_base = pnu_to_params(pnu)
    if params_base is None:
        return [{"PNU": pnu, "_status": "INVALID_PNU", "_memo": "PNU 형식 오류"}]

    url = f"{BASE_URL}/{OPERATION}"
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*", "Connection": "close"}

    async with sem:
        all_rows: List[Dict[str, Any]] = []
        total_count: Optional[int] = None
        page_no = 1
        backoff = 0.8

        while page_no <= max_pages:
            params = {
                "serviceKey": service_key,
                "_type": "json",
                "pageNo": page_no,
                "numOfRows": num_rows,
                **params_base,
            }
            ok = False
            last_err = ""

            for attempt in range(1, max_retries + 1):
                try:
                    await asyncio.sleep(random.uniform(0.05, 0.2))
                    async with session.get(url, params=params, headers=headers, timeout=timeout_sec) as resp:
                        txt = await resp.text()

                        if resp.status in (429, 500, 502, 503, 504):
                            last_err = f"HTTP {resp.status}"
                            if attempt < max_retries:
                                await asyncio.sleep(min(backoff, 20) + random.uniform(0, 0.5))
                                backoff = min(backoff * 2, 30)
                                continue
                            all_rows.append({"PNU": pnu, "_status": "HTTP_ERROR", "_memo": last_err, "_page": page_no})
                            ok = True
                            break

                        if resp.status != 200:
                            all_rows.append({"PNU": pnu, "_status": f"HTTP_{resp.status}", "_memo": txt[:200]})
                            ok = True
                            break

                        try:
                            data = json.loads(txt)
                        except Exception:
                            all_rows.append({"PNU": pnu, "_status": "JSON_ERROR", "_memo": txt[:200]})
                            ok = True
                            break

                        items, total = extract_items(data)
                        if total_count is None:
                            total_count = total

                        if not items:
                            if page_no == 1:
                                all_rows.append({"PNU": pnu, "_status": "NO_DATA", "_memo": "조회 결과 없음"})
                            ok = True
                            break

                        query_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        for it in items:
                            row = {"PNU": pnu, "_status": "OK", "queryAt": query_at}
                            row.update(it)
                            all_rows.append(row)

                        ok = True
                        break

                except asyncio.TimeoutError:
                    last_err = "timeout"
                except aiohttp.ClientError as e:
                    last_err = f"client: {e}"
                except Exception as e:
                    last_err = f"{type(e).__name__}: {e}"

                if attempt < max_retries:
                    await asyncio.sleep(min(backoff, 20) + random.uniform(0, 0.5))
                    backoff = min(backoff * 2, 30)

            if not ok:
                all_rows.append({"PNU": pnu, "_status": "HTTP_ERROR", "_memo": last_err})

            if total_count is None or total_count == 0:
                break
            pages_expected = (total_count + num_rows - 1) // num_rows
            if page_no >= pages_expected:
                break
            page_no += 1

        return all_rows


async def run_all(
    pnus: List[str],
    service_key: str,
    concurrency: int,
    cancel_flag: CancelFlag,
    log_cb,
    progress_cb,
) -> List[Dict[str, Any]]:
    sem = asyncio.Semaphore(concurrency)
    connector = aiohttp.TCPConnector(
        family=socket.AF_INET,
        limit=concurrency,
        limit_per_host=2,
        ttl_dns_cache=300,
        force_close=True,
        enable_cleanup_closed=True,
    )
    all_rows: List[Dict[str, Any]] = []
    total = len(pnus)
    done = 0
    t0 = time.time()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None), connector=connector) as session:
        tasks = [fetch_pnu(session, p, service_key, sem) for p in pnus]
        for coro in asyncio.as_completed(tasks):
            if cancel_flag.is_canceled:
                log_cb("취소 감지 → 중단")
                break
            rows = await coro
            all_rows.extend(rows)
            done += 1
            dt = max(time.time() - t0, 1e-6)
            progress_cb(done, total, done / dt)

    return all_rows


# =========================
# 결과 가공
# =========================
def build_sheets(all_rows: List[Dict[str, Any]]):
    df_raw = pd.DataFrame(all_rows)

    cols_exist = [c for c in OUTPUT_COLS if c in df_raw.columns]
    df_raw_out = df_raw[cols_exist].copy() if not df_raw.empty else pd.DataFrame(columns=OUTPUT_COLS)

    ok_mask = df_raw["_status"] == "OK" if "_status" in df_raw.columns else pd.Series([], dtype=bool)
    df_ok = df_raw[ok_mask].copy() if not df_raw.empty else pd.DataFrame()

    if df_ok.empty:
        df_detail = pd.DataFrame(columns=OUTPUT_COLS)
        df_summary = pd.DataFrame(columns=SUMMARY_COLS)
        return df_summary, df_detail, df_raw_out

    df_detail = df_ok.copy()
    if "hhldCnt" in df_detail.columns:
        df_detail["hhldCnt"] = pd.to_numeric(df_detail["hhldCnt"], errors="coerce").fillna(0).astype(int)

    grp = df_detail.groupby("PNU", sort=False)
    summary_rows = []
    for pnu, g in grp:
        row = {
            "PNU": pnu,
            "지번주소": g["platPlc"].iloc[0] if "platPlc" in g else "",
            "도로명주소": g["newPlatPlc"].iloc[0] if "newPlatPlc" in g else "",
            "건물명": " / ".join(g["bldNm"].dropna().astype(str).unique()) if "bldNm" in g else "",
            "주용도명": " / ".join(g["mainPurpsCdNm"].dropna().astype(str).unique()) if "mainPurpsCdNm" in g else "",
            "세대수_합계": int(g["hhldCnt"].sum()) if "hhldCnt" in g else 0,
            "건물동수": len(g),
            "최신_데이터생성일자": g["crtnDay"].dropna().astype(str).max() if "crtnDay" in g else "",
            "조회일시": g["queryAt"].dropna().astype(str).iloc[0] if "queryAt" in g else "",
        }
        summary_rows.append(row)

    df_summary = pd.DataFrame(summary_rows, columns=SUMMARY_COLS)
    return df_detail



def make_excel_bytes(df_detail: pd.DataFrame) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        if df_detail.empty:
            pd.DataFrame([{"결과": "상세 데이터 없음"}]).to_excel(writer, index=False, sheet_name="pnu별 건축물대장 상세")
        else:
            cols_exist = [c for c in OUTPUT_COLS if c in df_detail.columns]
            df_detail_out = df_detail[cols_exist].copy().rename(columns=COLUMN_RENAME_MAP)
            df_detail_out.to_excel(writer, index=False, sheet_name="pnu별 건축물대장 상세")

    return output.getvalue()


# =========================
# Streamlit UI
# =========================
st.set_page_config(page_title="PNU 기반 건축물대장 조회기", page_icon="🏢", layout="wide")

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.2rem; padding-bottom: 2rem; max-width: 1200px;}
    .app-card {
        background: #ffffff; border: 1px solid #e5e7eb; border-radius: 16px;
        padding: 18px 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.04); margin-bottom: 16px;
    }
    .app-title {font-size: 1.5rem; font-weight: 700; margin-bottom: 0.2rem;}
    .app-sub {color: #6b7280; font-size: 0.95rem;}
    .metric-row {
        display: grid; grid-template-columns: repeat(4, minmax(0,1fr)); gap: 12px; margin-top: 8px;
    }
    .metric-box {
        border: 1px solid #e5e7eb; border-radius: 14px; padding: 14px 16px; background: #fafafa;
    }
    .metric-label {font-size: 0.82rem; color: #6b7280; margin-bottom: 4px;}
    .metric-value {font-size: 1.2rem; font-weight: 700; color: #111827;}
    div[data-testid="stFileUploader"] section {padding: 0.5rem 0.75rem;}
    div[data-testid="stDownloadButton"] button {
        background-color: #2563eb !important;
        color: white !important;
        font-weight: 600;
        font-size: 1rem;
        border-radius: 10px;
        height: 48px;
    }
    div[data-testid="stDownloadButton"] button:hover {
        background-color: #1d4ed8 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="app-card">
      <div class="app-title">PNU 기반 건축물대장 조회기</div>
      <div class="app-sub">PNU 엑셀을 업로드하면 건축물대장 표제부를 조회해 요약 시트와 상세 시트를 엑셀로 내려받습니다.</div>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.container(border=True):
    c1, c2 = st.columns([2.2, 1.1])
    with c1:
        service_key = st.text_input("서비스키(Decoding)", value="", type="password", placeholder="공공데이터포털 Decoding 서비스키 입력")
        uploaded_file = st.file_uploader("입력 엑셀(.xlsx)", type=["xlsx"])
        with open("입력양식.xlsx", "rb") as f:
            st.download_button(
                "입력양식 다운로드",
                data=f.read(),
                file_name="입력양식.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
    with c2:
        concurrency = st.number_input("동시요청 수", min_value=1, max_value=20, value=10, step=1)
        dedup = st.checkbox("중복 PNU 제거", value=True)
        run_clicked = st.button("실행", type="primary", use_container_width=True)

    st.caption("입력양식.xlsx 형식으로 pnu 컬럼(19자리)을 입력하세요. 출력 파일은 Sheet1=PNU별 요약, Sheet2=건축물 상세로 구성됩니다.")

if run_clicked:
    if not service_key.strip():
        st.error("서비스키를 입력하세요.")
        st.stop()
    if uploaded_file is None:
        st.error("입력 엑셀(.xlsx)을 업로드하세요.")
        st.stop()

    try:
        df_in = pd.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"엑셀 파일을 읽지 못했습니다: {e}")
        st.stop()

    if "pnu" not in df_in.columns:
        st.error("엑셀에 'pnu' 컬럼이 없습니다.")
        st.stop()

    log_lines: List[str] = []
    progress_bar = st.progress(0)
    status_box = st.empty()
    log_box = st.empty()

    def log_cb(message: str) -> None:
        log_lines.append(f"[{time.strftime('%H:%M:%S')}] {message}")
        log_box.code("\n".join(log_lines[-200:]), language="text")

    def progress_cb(done: int, total: int, rate: float) -> None:
        pct = int((done / max(total, 1)) * 100)
        progress_bar.progress(pct)
        status_box.info(f"{done}/{total} PNU 완료 | {rate:.1f} pnu/s")

    df_in["pnu"] = normalize_pnu_series(df_in["pnu"])
    valid = df_in["pnu"].str.fullmatch(r"\d{19}")
    pnus = df_in.loc[valid, "pnu"].astype(str).tolist()
    invalid_count = int((~valid).sum())
    log_cb(f"유효 PNU {len(pnus)}건 / 무효 {invalid_count}건")

    if dedup:
        before = len(pnus)
        pnus = list(dict.fromkeys(pnus))
        log_cb(f"중복 제거: {before} → {len(pnus)}")

    if not pnus:
        st.error("조회할 유효 PNU가 없습니다.")
        st.stop()

    cancel_flag = CancelFlag()
    status_box.info("실행 중...")

    all_rows = asyncio.run(
        run_all(
            pnus=pnus,
            service_key=service_key.strip(),
            concurrency=int(concurrency),
            cancel_flag=cancel_flag,
            log_cb=log_cb,
            progress_cb=progress_cb,
        )
    )

    log_cb(f"API 완료: 총 {len(all_rows)}행 수신")
    df_detail = build_sheets(all_rows)
    ok_cnt = int(df_detail["PNU"].nunique()) if not df_detail.empty else 0
    log_cb(f"건축물 있음 {ok_cnt}건")

    excel_bytes = make_excel_bytes(df_detail)
    status_box.success("완료")
    progress_bar.progress(100)

    st.markdown(
        f"""
        <div class="metric-row" style="grid-template-columns: repeat(3, minmax(0,1fr));">
            <div class="metric-box"><div class="metric-label">입력 지번 수</div><div class="metric-value">{len(df_in):,}</div></div>
            <div class="metric-box"><div class="metric-label">건축물 있음</div><div class="metric-value">{df_detail['PNU'].nunique() if not df_detail.empty else 0:,}</div></div>
            <div class="metric-box"><div class="metric-label">무효 PNU</div><div class="metric-value">{invalid_count:,}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)
    st.download_button(
        "📥 엑셀 다운로드",
        data=excel_bytes,
        file_name="pnu_건축물대장_조회결과.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
    )

    tab1, tab2 = st.tabs(["건축물 상세", "실행 로그"])
    with tab1:
        if df_detail.empty:
            st.warning("상세 데이터가 없습니다.")
        else:
            cols_exist = [c for c in OUTPUT_COLS if c in df_detail.columns]
            df_detail_out = df_detail[cols_exist].copy().rename(columns=COLUMN_RENAME_MAP)
            st.dataframe(df_detail_out, use_container_width=True, hide_index=True)
    with tab2:
        st.code("\n".join(log_lines[-500:]) if log_lines else "로그 없음", language="text")
