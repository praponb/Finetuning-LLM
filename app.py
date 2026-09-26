import os
import re

import requests
import streamlit as st

# Ollama server; in Docker this points at the host (see docker-compose.yml)
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434").rstrip("/")


def extract_code(text: str) -> str:
    """Return the longest fenced code block in the reply (or the whole reply if there is none)."""
    blocks: list[str] = re.findall(r"```(?:python|pyspark|py)?\s*\n(.*?)```", text, flags=re.S)
    if blocks:
        return max(blocks, key=len).strip()
    # unterminated fence (generation stopped before the closing ```)
    text = re.sub(r"^\s*```(?:python|pyspark|py)?\s*\n", "", text)
    return text.replace("```", "").strip()


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
                            "options": {
                                "temperature": 0.2
                            }
                        },
                        timeout=120
                    )

                    if response.status_code == 200:
                        payload = response.json()
                        pyspark_output = payload.get("message", {}).get("content", "")
                        
                        result_container.code(extract_code(pyspark_output), language="python")
                    else:
                        st.error(f"Ollama returned error code {response.status_code}: {response.text}")

                except requests.exceptions.ConnectionError:
                    st.error(f"Cannot connect to Ollama at {OLLAMA_URL}. Make sure the Ollama service is running (`open -a Ollama`).")
                except Exception as e:
                    st.error(f"An unexpected error occurred: {str(e)}")
    else:
        result_container.info("Click **Convert to PySpark** to generate code.")
