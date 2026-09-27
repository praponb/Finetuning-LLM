# 05 - Serving & Streamlit UI

The user-facing layer connects a local **Ollama** inference server with a small **Streamlit** web application.

---

## 🔌 Architecture & Flow

```mermaid
sequenceDiagram
    autonumber
    actor User
    participant Browser as Streamlit Web App (app.py)
    participant Ollama as Ollama Server (OLLAMA_URL, default :11434)
    participant Model as sql2pyspark (Q8_0 GGUF)

    User->>Browser: Enters SQL query in the left text area
    User->>Browser: Clicks "Convert to PySpark"
    Browser->>Browser: Validates non-empty input
    Browser->>Ollama: POST /api/chat with prompt & model tag
    Ollama->>Model: Generates tokens (temperature 0.2)
    Model-->>Ollama: Markdown reply with a ```python block
    Ollama-->>Browser: JSON response {message: {content: ...}}
    Browser->>Browser: extract_code() pulls out the code block
    Browser-->>User: Renders highlighted PySpark code
```

---

## 🦙 Ollama Serving Engine

Each export registers `sql2pyspark.gguf` in the local Ollama daemon under its tag. `sql2pyspark:latest` points to **v5** (since 2026-09-28); `sql2pyspark:v3` (the previous `latest`), `sql2pyspark:v4`, `sql2pyspark:v2` and `sql2pyspark:v1` stay available. Ollama keeps its own copy of each tag; `sql2pyspark.gguf` on disk holds the last export (v5).

### Server Address
- Default: `http://localhost:11434`
- Override with the `OLLAMA_URL` environment variable (read by both `app.py` and `eval/evaluate.py`), e.g. `OLLAMA_URL=http://gpu-box:11434 make app`.

### API Endpoint
- **URL**: `$OLLAMA_URL/api/chat`
- **Method**: `POST`
- **Headers**: `Content-Type: application/json`

#### Request Payload (as sent by `app.py`):
```json
{
  "model": "sql2pyspark",
  "messages": [
    {
      "role": "user",
      "content": "Convert SQL to PySpark:\n```sql\nSELECT dept_id, COUNT(*) AS total FROM employees GROUP BY dept_id;\n```"
    }
  ],
  "stream": false,
  "options": {
    "temperature": 0.2
  }
}
```
The system prompt and `top_p 0.9` come from the Modelfile, so the app doesn't send them.

#### Response Structure (example):
```json
{
  "model": "sql2pyspark",
  "created_at": "2026-09-26T07:12:44.123456Z",
  "message": {
    "role": "assistant",
    "content": "```python\nfrom pyspark.sql import functions as F\n\nresult = df_employees.groupBy(\"dept_id\").agg(F.count(\"*\").alias(\"total\"))\n```"
  },
  "done": true
}
```

---

## 🖥️ Streamlit Frontend Application (`app.py`)

[app.py](../app.py) is a small (~100 lines) Streamlit frontend with two dependencies (`streamlit`, `requests`).

### 1. Robust Code Extraction (`extract_code`)
Models sometimes stop before closing a code fence. `extract_code()` handles both cases:
```python
def extract_code(text: str) -> str:
    """Return the longest fenced code block in the reply (or the whole reply if there is none)."""
    blocks: list[str] = re.findall(r"```(?:python|pyspark|py)?\s*\n(.*?)```", text, flags=re.S)
    if blocks:
        return max(blocks, key=len).strip()
    # unterminated fence (generation stopped before the closing ```)
    text = re.sub(r"^\s*```(?:python|pyspark|py)?\s*\n", "", text)
    return text.replace("```", "").strip()
```
`eval/evaluate.py` uses an identical copy of this function, so the UI shows the same code the evaluation scored.

### 2. Two-Column Layout
- **Left column**: text area preloaded with an example query (`COUNT(*)`, `AVG()`, `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`).
- **Right column**: generated code with syntax highlighting and Streamlit's copy button.

### 3. Model Tag Field (Sidebar)
A text field (default `sql2pyspark`, i.e. `latest`) accepts any tag from `ollama list`, for example:
- `sql2pyspark:latest` (default, currently v5)
- `sql2pyspark:v5` (= `latest`; best on the fixed test set and the v4 features; see [Evaluation](04-evaluation-and-benchmarking.md))
- `sql2pyspark:v4` (better at the v4 features: GROUPING SETS, PIVOT/UNPIVOT, nested data, ...; see [Evaluation](04-evaluation-and-benchmarking.md))
- `sql2pyspark:v3`
- `sql2pyspark:v2` (previous model, for comparison)
- `sql2pyspark:v1` (template-based first model)

### Generation settings
`app.py` sends `temperature` 0.2. For SQL longer than 2,000 characters, `generation_options()` also sets `repeat_last_n` to 2,048 (Ollama's default: 64) and the request timeout rises from 120 s to 300 s: on long multi-CTE queries the model otherwise repeats window definitions until the token limit. Short queries are unaffected (see the repetition-penalty experiment in [Evaluation](04-evaluation-and-benchmarking.md)).

### 4. Error Handling
- **Ollama unreachable**: shows `Cannot connect to Ollama at <OLLAMA_URL>. Make sure the Ollama service is running (open -a Ollama).`
- **Blank input**: shows a warning.
- **Timeout**: 120 seconds per request (300 seconds for SQL longer than 2,000 characters).

The app was checked headlessly with the v2 model (`streamlit.testing.v1.AppTest`) on the default query and three hold-out queries (a CTE with a join, a PIVOT, and a GROUP BY/ORDER BY). All three hold-out conversions were execution-verified as correct.

---

## 🚀 Launching the Interface

```bash
make app
# or
streamlit run app.py
```

The UI runs at **`http://localhost:8501`**.

### From Docker
```bash
docker compose up -d --build     # app container on http://localhost:8501, model served by the host's Ollama
docker compose down
```
The `app` container reaches the native Ollama on the host through `OLLAMA_URL=http://host.docker.internal:11434` (the default in `docker-compose.yml`) and reports healthy once Streamlit is up. Keep Ollama native on a Mac: containers can't use the Apple GPU. See the [Operations Runbook](07-operations-runbook.md#-docker) for the other profiles.
