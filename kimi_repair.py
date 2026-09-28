"""Kimi K3 repair loop shared by the Streamlit app (app.py) and the evaluation (eval/agent_repair.py).

Kimi K3 (Moonshot AI) gets a Spark SQL query and a PySpark translation that may be wrong, plus one tool that runs
PySpark code and reports the error or the result. It fixes the code and replies with the final version. The tool
is supplied by the caller:
  - the evaluation runs the code on hand-made sample tables (eval/real_tables.py);
  - the app has no data, so it runs the code on EMPTY tables whose schemas Kimi declares from the SQL
    (EmptyTableRunner). That catches syntax, name, column and type errors and compares the output columns with
    the SQL's, but not mistakes that only show in the data.

The key is KIMI_API_KEY (environment or the repo's .env, gitignored; template .env.example).
"""
from __future__ import annotations

import ast
import builtins
import json
import os
import re
import time
from pathlib import Path
from typing import Callable

SQL_CALL = re.compile(r"\.sql\s*\(")
DEFAULT_BASE_URL = "https://api.moonshot.ai/v1"
DEFAULT_MODEL = "kimi-k3"
# USD per million tokens: (input cache miss, input cache hit, output); Kimi pricing page, 2026-09
PRICES = {"kimi-k3": (3.0, 0.30, 15.0)}
PREDEFINED = {"F", "Window", "spark"}
MAX_ROWS, MAX_CHARS = 20, 6000

RULES = """Rules for the final code:
- Use the DataFrame API only; never call spark.sql(...) or any .sql(...) method (F.expr is fine).
- Each input table is bound to a variable named df_<table>; `spark`, `F` (pyspark.sql.functions) and `Window`
  are also defined.
- Assign the final DataFrame to a variable named `result`, with the same columns (names and order) as the
  query's SELECT list. Keep the query's semantics exactly; do not add or drop rows or columns."""

SYSTEM_SAMPLE = f"""You fix PySpark code that was meant to be an exact translation of a Spark SQL query.

{RULES}

You can check your work with the run_pyspark tool, which runs a snippet against small sample tables and shows the
error or the first rows of `result`. You do not know the expected output, so reason about whether the rows look
right for the SQL. The tool has a limited number of runs; use them on the fixes that matter. When you are done,
reply with the complete final code in one ```python block."""

SYSTEM_EMPTY = f"""You fix PySpark code that was meant to be an exact translation of a Spark SQL query.

{RULES}

There is no sample data. The run_pyspark tool runs your code on EMPTY tables whose schemas you declare in its
`tables` argument (table name -> Spark DDL column list, e.g. "orders": "order_id BIGINT, amount DOUBLE"). Infer
the columns and types each table needs from the SQL. The tool also analyzes the SQL itself on those tables and
compares its output columns with those of `result`, so it catches syntax, name, column, type and column-order
errors, but not mistakes that only show in the data: reason about those yourself. Tables you declared stay
declared for later runs. The tool has a limited number of runs. When you are done, reply with the complete final
code in one ```python block."""

CODE_PARAM = {"type": "string", "description": "Complete Python code; it must assign the final DataFrame to `result`."}
TOOL_SAMPLE = {"type": "function", "function": {
    "name": "run_pyspark",
    "description": "Run a PySpark snippet against the sample tables and return the error or the first rows of `result`.",
    "parameters": {"type": "object", "properties": {"code": CODE_PARAM}, "required": ["code"]}}}
TOOL_EMPTY = {"type": "function", "function": {
    "name": "run_pyspark",
    "description": "Run a PySpark snippet on empty tables with the declared schemas; report errors and compare the "
                   "columns of `result` with the SQL's output columns.",
    "parameters": {"type": "object", "properties": {
        "code": CODE_PARAM,
        "tables": {"type": "object", "additionalProperties": {"type": "string"},
                   "description": "Table name -> Spark DDL column list. Only needed on the first run or to change "
                                  "a schema; earlier declarations are kept."}},
        "required": ["code"]}}}


def load_env(path: Path) -> None:
    """KEY=VALUE lines from .env into os.environ (values already set in the environment win)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def extract_code(text: str) -> str:
    """Return the longest fenced code block in the reply (or the whole reply if there is none)."""
    blocks: list[str] = re.findall(r"```(?:python|pyspark|py)?\s*\n(.*?)```", text, flags=re.S)
    if blocks:
        return max(blocks, key=len).strip()
    # unterminated fence (generation stopped before the closing ```)
    text = re.sub(r"^\s*```(?:python|pyspark|py)?\s*\n", "", text)
    return text.replace("```", "").strip()


def short_error(e: BaseException) -> str:
    msg = str(e).strip().splitlines()[0] if str(e).strip() else ""
    return f"{type(e).__name__}: {msg}"[:400]


def static_check(code: str) -> str | None:
    """Problems visible without Spark: a syntax error, names used but never defined, or no `result`.
    Returns None when nothing is found (the code may still be wrong)."""
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        return f"SyntaxError: {e.msg} (line {e.lineno})"
    defined = set(PREDEFINED) | set(dir(builtins))
    for n in ast.walk(tree):
        if isinstance(n, ast.Name) and isinstance(n.ctx, (ast.Store, ast.Del)):
            defined.add(n.id)
        elif isinstance(n, ast.ClassDef):
            defined.add(n.name)
        elif isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            if not isinstance(n, ast.Lambda):
                defined.add(n.name)
            a = n.args
            defined.update(x.arg for x in a.posonlyargs + a.args + a.kwonlyargs + [a.vararg, a.kwarg] if x)
        elif isinstance(n, (ast.Import, ast.ImportFrom)):
            defined.update((a.asname or a.name).split(".")[0] for a in n.names)
        elif isinstance(n, ast.ExceptHandler) and n.name:
            defined.add(n.name)
    undefined = sorted({n.id for n in ast.walk(tree) if isinstance(n, ast.Name) and isinstance(n.ctx, ast.Load)
                        and n.id not in defined and not n.id.startswith("df_")})
    if undefined:
        return "Undefined names: " + ", ".join(undefined[:10])
    if "result" not in defined:
        return "The code never assigns `result`"
    return None


def exec_code(code: str, namespace: dict):
    """Run the snippet and return its `result` DataFrame (raises on errors and on a missing `result`)."""
    from pyspark.sql import DataFrame
    ns = dict(namespace)
    exec(compile(code, "<pyspark>", "exec"), ns)
    res = ns.get("result")
    if not isinstance(res, DataFrame):
        raise LookupError("code did not assign a DataFrame to `result`")
    return res


class EmptyTableRunner:
    """The app's tool: run code on empty tables with Kimi-declared schemas and compare with the SQL's columns."""

    def __init__(self, spark, sql: str):
        self.spark, self.sql = spark, sql.strip().rstrip(";")
        self.tables: dict[str, str] = {}

    def namespace(self) -> dict:
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        ns = {"F": F, "Window": Window, "spark": self.spark}
        for name, ddl in self.tables.items():
            df = self.spark.createDataFrame([], ddl)
            df.createOrReplaceTempView(name)
            ns[f"df_{name.split('.')[-1]}"] = df
        return ns

    def __call__(self, args: dict) -> str:
        code = args.get("code")
        if not isinstance(code, str):
            return 'Invalid arguments: expected {"code": "<python code>", "tables": {...}}.'
        if SQL_CALL.search(code):
            return "Refused: the code calls .sql(...). Translate the query to the DataFrame API instead."
        previous = dict(self.tables)
        if isinstance(args.get("tables"), dict):
            self.tables.update({str(k): str(v) for k, v in args["tables"].items()})
        if not self.tables:
            return "No tables declared yet: pass `tables` (table name -> Spark DDL column list)."
        try:
            ns = self.namespace()
        except Exception as e:  # noqa: BLE001
            self.tables = previous      # keep only schemas that worked
            return f"Invalid table schema (not kept): {short_error(e)}"
        try:
            sql_cols = self.spark.sql(self.sql).columns
            sql_note = f"SQL output columns: {sql_cols}"
        except Exception as e:  # noqa: BLE001
            sql_cols, sql_note = None, f"The SQL itself did not analyze on these tables ({short_error(e)}); " \
                                       "fix the schemas if a column or table is missing."
        try:
            res = exec_code(code, ns)
            res.collect()
        except Exception as e:  # noqa: BLE001
            return f"Error: {short_error(e)}\n{sql_note}"
        verdict = "" if sql_cols is None else \
            ("columns match the SQL" if res.columns == sql_cols else "COLUMN MISMATCH with the SQL")
        return f"OK (empty tables). result columns: {res.columns}\n{sql_note}\n{verdict}".strip()


def cost(model: str, u: dict) -> float:
    p_miss, p_hit, p_out = PRICES.get(model, (0.0, 0.0, 0.0))
    return ((u["input"] - u["cache_read"]) * p_miss + u["cache_read"] * p_hit + u["output"] * p_out) / 1e6


def make_client():
    """OpenAI-compatible client for Kimi (reads KIMI_API_KEY / KIMI_BASE_URL)."""
    from openai import OpenAI
    return OpenAI(api_key=os.environ["KIMI_API_KEY"], timeout=900,
                  base_url=os.environ.get("KIMI_BASE_URL") or DEFAULT_BASE_URL)


def repair(client, model: str, effort: str, sql: str, prior_code: str, run_tool: Callable[[dict], str],
           max_runs: int = 5, system: str = SYSTEM_EMPTY, tool: dict = TOOL_EMPTY, context: str = "",
           on_event: Callable[[str], None] | None = None) -> dict:
    """Let Kimi fix `prior_code`. `run_tool(args)` executes one tool call and returns the text Kimi sees."""
    runs: list[dict] = []

    def call_tool(arguments: str) -> str:
        try:
            args = json.loads(arguments)
            assert isinstance(args, dict)
        except (json.JSONDecodeError, AssertionError):
            return 'Invalid arguments: expected a JSON object with "code".'
        if len(runs) >= max_runs:
            return f"No runs left ({max_runs} used). Reply with your final code now."
        out = run_tool(args)
        runs.append({"code": args.get("code", ""), "output": out[:500]})
        if on_event:
            on_event(f"run {len(runs)}: {out.splitlines()[0][:150] if out else ''}")
        return out + f"\n[run {len(runs)} of {max_runs}]"

    prompt = (f"Spark SQL query:\n```sql\n{sql}\n```\n\n{context}"
              f"Current PySpark translation (from a small fine-tuned model; it may be wrong or not run):\n"
              f"```python\n{prior_code}\n```\n\n"
              "Check it with run_pyspark, fix what is wrong, and reply with the complete final code.")
    messages: list = [{"role": "system", "content": system}, {"role": "user", "content": prompt}]
    usage = {"input": 0, "output": 0, "cache_read": 0}
    final_text, stop = "", None
    t0 = time.time()
    for _ in range(max_runs + 3):
        # K3 always thinks; temperature/top_p are fixed server-side, so none are sent
        resp = client.chat.completions.create(model=model, messages=messages, tools=[tool],
                                              reasoning_effort=effort, max_tokens=32768)
        u = resp.usage
        if u:
            usage["input"] += u.prompt_tokens or 0
            usage["output"] += u.completion_tokens or 0
            details = getattr(u, "prompt_tokens_details", None)
            usage["cache_read"] += (getattr(details, "cached_tokens", None) if details else None) \
                or getattr(u, "cached_tokens", None) or 0
        choice = resp.choices[0]
        msg, stop = choice.message, choice.finish_reason
        # pass the assistant message back unchanged (it carries reasoning_content for the next turn)
        messages.append(msg.model_dump(exclude_none=True))
        if msg.content and msg.content.strip():
            final_text = msg.content
        if not msg.tool_calls:
            break
        for tc in msg.tool_calls:
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": call_tool(tc.function.arguments)})
    code = extract_code(final_text) if "```" in final_text else ""
    if not code and runs:          # no code block in the final reply: fall back to the last code it ran
        code = runs[-1]["code"]
    return {"final_reply": final_text, "code": code, "runs": runs, "stop_reason": stop, "usage": usage,
            "cost_usd": round(cost(model, usage), 4), "seconds": round(time.time() - t0, 1)}
