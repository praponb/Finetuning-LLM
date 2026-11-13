import os
from pathlib import Path

import requests
import streamlit as st

import kimi_repair as kr

# Ollama server; in Docker this points at the host (see docker-compose.yml)
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")
LONG_SQL_CHARS = 2000
# Kimi K3 repair (optional): KIMI_API_KEY from the environment or .env (Docker passes .env in)
kr.load_env(Path(__file__).resolve().parent / ".env")
KIMI_READY = bool(os.environ.get("KIMI_API_KEY"))
KIMI_MODEL = os.environ.get("KIMI_MODEL") or kr.DEFAULT_MODEL
KIMI = kr.display_name(KIMI_MODEL)      # e.g. "Kimi K3.1 Pro", used in every label
REPAIR_MODES = ["When a check fails or the SQL is long", "When a check fails", "Always", "Off"]


def generation_options(sql: str) -> dict:
    """Ollama options for one request. On long multi-CTE SQL the model can repeat window definitions until it runs
    out of tokens; a repetition window of 2,048 tokens (Ollama's default is 64) stops that. It is applied to long
    inputs only, because on short queries it cost one hold-out query (docs/04, repetition-penalty experiment)."""
    options = {"temperature": 0.2}
    if len(sql) > LONG_SQL_CHARS:
        options["repeat_last_n"] = 2048
    return options


extract_code = kr.extract_code


@st.cache_resource(show_spinner="Starting Spark for the code checks...")
def get_spark():
    """Local Spark session for Kimi's tool (empty tables only, so one core and little memory are enough)."""
    import sys
    from pyspark.sql import SparkSession
    # Python workers must use the app's own interpreter: `python3` on PATH can be another minor version
    # (Homebrew 3.15 vs the 3.14 venv on the Mac mini) -> PYTHON_VERSION_MISMATCH in every Kimi check.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    spark = (SparkSession.builder.master("local[1]").appName("sql2pyspark-app")
             .config("spark.ui.enabled", "false").config("spark.ui.showConsoleProgress", "false")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.driver.memory", "1g").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def run_repair(sql: str, code: str) -> None:
    """Kimi fixes `code`, checking its versions on empty tables; the result goes to session state."""
    with st.status(f"{KIMI} is checking and repairing the code...", expanded=True) as status:
        try:
            runner = kr.EmptyTableRunner(get_spark(), sql)
            r = kr.repair(kr.make_client(), KIMI_MODEL, "high", sql, code, runner, max_runs=5,
                          on_event=lambda line: st.write(line))
        except Exception as e:  # noqa: BLE001
            status.update(label=f"{KIMI} repair failed", state="error")
            st.error(f"{KIMI} repair failed: {kr.short_error(e)}")
            return
        last = r["runs"][-1]["output"] if r["runs"] else ""
        verified = bool(r["runs"]) and r["runs"][-1]["code"].strip() == r["code"].strip() \
            and last.startswith("OK") and "MISMATCH" not in last
        status.update(label=f"{KIMI} finished: {len(r['runs'])} checks, {r['seconds']:.0f} s, ${r['cost_usd']:.2f}",
                      state="complete", expanded=False)
    # after the verified check, so a version that only gained import lines still counts as checked
    st.session_state["repair"] = {**r, "code": kr.add_imports(r["code"]), "verified": verified}


# Page setup
st.set_page_config(
    page_title="SQL to PySpark Converter",
    page_icon="⚡",
    layout="wide"
)

st.title("⚡ SQL to PySpark Converter")
st.caption("Powered by your fine-tuned `sql2pyspark` model running locally on Ollama.")

with st.sidebar:
    model_tag = st.text_input(
        "Ollama model",
        value="sql2pyspark",
        help="Any tag from `ollama list`, e.g. sql2pyspark:v1 to compare with the previous model.",
    )
    st.subheader(f"{KIMI} repair")
    if KIMI_READY:
        repair_mode = st.radio(
            "Run the repair", REPAIR_MODES, index=0,
            help=f"{KIMI} (Moonshot AI) checks the code on empty tables with the schemas the SQL implies and fixes "
                 "syntax, name, column and type errors. About $0.10-0.30 and 1-5 minutes per repair. Long SQL "
                 f"(> {LONG_SQL_CHARS:,} characters) is repaired by default: the model's errors there often only show "
                 "when the code runs.",
        )
    else:
        repair_mode = "Off"
        st.caption("Off: put `KIMI_API_KEY=...` in a `.env` file in the repo root and restart the app to enable it.")

# Layout: 2 side-by-side columns
col1, col2 = st.columns(2)

default_sql = """SELECT department, 
       COUNT(*) AS total_employees, 
       AVG(salary) AS avg_salary
FROM employees 
WHERE status = 'ACTIVE' 
GROUP BY department 
HAVING COUNT(*) > 5 
ORDER BY avg_salary DESC;"""

with col1:
    st.subheader("Input SQL Query")
    sql_input = st.text_area(
        label="SQL Input",
        value=default_sql,
        height=320,
        label_visibility="collapsed"
    )
    convert_btn = st.button("🚀 Convert to PySpark", type="primary", width="stretch")

with col2:
    st.subheader("Generated PySpark Code")
    result_container = st.empty()

    if convert_btn:
        if not sql_input.strip():
            st.warning("Please provide a SQL query to convert.")
        else:
            with st.spinner("Translating query via Ollama..."):
                try:
                    # Call local Ollama API
                    response = requests.post(
                        f"{OLLAMA_URL}/api/chat",
                        json={
                            "model": model_tag,
                            "messages": [
                                {
                                    "role": "user",
                                    "content": f"Convert SQL to PySpark:\n```sql\n{sql_input}\n```"
                                }
                            ],
                            "stream": False,
                            "options": generation_options(sql_input),
                        },
                        timeout=600 if len(sql_input) > LONG_SQL_CHARS else 180
                    )

                    if response.status_code == 200:
                        payload = response.json()
                        pyspark_output = payload.get("message", {}).get("content", "")
                        st.session_state["converted"] = {"sql": sql_input, "code": extract_code(pyspark_output)}
                        st.session_state.pop("repair", None)
                        st.session_state["auto_repair"] = True
                    else:
                        st.error(f"Ollama returned error code {response.status_code}: {response.text}")

                except requests.exceptions.ConnectionError:
                    st.error(f"Cannot connect to Ollama at {OLLAMA_URL}. Make sure the Ollama service is running (`open -a Ollama`).")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {str(e)}")

    converted = st.session_state.get("converted")
    if converted:
        result_container.code(converted["code"], language="python")
        problem = kr.static_check(converted["code"], converted["sql"])
        if problem:
            st.warning(f"The model's code has a problem: {problem}")
        else:
            st.caption("✅ Basic checks passed (syntax, defined names, `result`, UNION column order); "
                       "run it on your data to be sure.")
        long_sql = len(converted["sql"]) > LONG_SQL_CHARS
        wanted = (repair_mode == "Always"
                  or (repair_mode.startswith("When a check fails") and problem)
                  or (repair_mode == "When a check fails or the SQL is long" and long_sql))
        if st.session_state.pop("auto_repair", False) and wanted:
            run_repair(converted["sql"], converted["code"])
        elif KIMI_READY and "repair" not in st.session_state and \
                st.button(f"🛠 Check and repair with {KIMI}", width="stretch"):
            run_repair(converted["sql"], converted["code"])
        rep = st.session_state.get("repair")
        if rep:
            st.subheader(f"Repaired by {KIMI}")
            st.code(rep["code"], language="python")
            if rep["verified"]:
                st.success("This version ran on empty tables with the SQL's schemas and returns the SQL's columns.")
            else:
                st.info("Kimi's last check did not cover this exact version; review it before use.")
            u = rep["usage"]
            st.caption(f"{len(rep['runs'])} checks · {rep['seconds']:.0f} s · {u['input']:,} input tokens "
                       f"({u['cache_read']:,} cached) · {u['output']:,} output tokens · ${rep['cost_usd']:.2f}")
            with st.expander("Kimi's checks"):
                for i, run in enumerate(rep["runs"], 1):
                    st.text(f"Run {i}: {run['output']}")
    else:
        result_container.info("Click **Convert to PySpark** to generate code.")
