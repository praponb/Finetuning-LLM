"""v19 option C: long "real-style" SQL -> PySpark pairs written by Kimi K3.1 Pro and verified in Spark.

For each case Kimi gets a domain, a sampled checklist of 8-12 SQL features (weighted toward the constructs the
fine-tuned model fails on in eval/real_queries.jsonl) and a few style knobs. It invents 3-5 tables (nested types
allowed) with small sample data written as Spark SQL `SELECT * FROM VALUES ...`, a long multi-CTE query and its
PySpark translation in the house style. A `check_case` tool runs everything in Spark: the SQL and the PySpark must
return the same columns and rows (verify_pairs.compare), with 3-60 rows. Only cases with a passing check are kept;
the last passing check is the case.

Run from sql2pyspark-mlx/ (reads KIMI_API_KEY from ../.env):
    .venv/bin/python realstyle_v19/generate.py --n 100 --split train --workers 8 --budget 15 --out realstyle_v19/pilot
Outputs <out>/cases.jsonl (passing cases), <out>/failed.jsonl, <out>/report.json.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
MLX = HERE.parent
REPO = MLX.parent
sys.path.insert(0, str(MLX))
sys.path.insert(0, str(REPO))

import kimi_repair as kr  # noqa: E402
from verify_pairs import compare, rows_of  # noqa: E402

# ---------------------------------------------------------------------------------------------------------------
# What to ask for
# ---------------------------------------------------------------------------------------------------------------
# (feature, weight). Weight 3 = rare in the training mix and/or seen failing on the real queries.
FEATURES = [
    ("an ARRAY<STRUCT<...>> column flattened with LATERAL VIEW explode (or posexplode), then struct fields read "
     "from the exploded alias (e.g. item.sku)", 3),
    ("a STRUCT column whose nested fields are read with dot notation (e.g. o.shipping_address.country)", 3),
    ("a MAP<STRING, ...> column read with element_at / map_keys / map_values or exploded into key/value rows", 3),
    ("a JSON string column parsed with from_json (explicit schema) or get_json_object", 3),
    ("higher-order array functions: TRANSFORM, FILTER, AGGREGATE or EXISTS with lambdas", 3),
    ("COLLECT_LIST(DISTINCT ...) or COLLECT_SET, optionally wrapped in ARRAY_SORT / SIZE", 3),
    ("SUM(DISTINCT ...) or AVG(DISTINCT ...) next to COUNT(DISTINCT ...)", 3),
    ("aggregate FILTER (WHERE ...) clauses, e.g. COUNT(*) FILTER (WHERE status = 'X')", 3),
    ("a SQL PIVOT clause with string values (e.g. FOR metric IN ('TEMPERATURE' AS temperature, ...))", 3),
    ("a SQL UNPIVOT clause turning several columns into name/value rows", 3),
    ("a window frame with RANGE BETWEEN (e.g. INTERVAL-based or numeric range)", 3),
    ("a self-referencing alias star after a join: SELECT a.*, b.col FROM ... (alias.* on a CTE)", 3),
    ("QUALIFY to filter on a window function", 2),
    ("GROUPING SETS / ROLLUP / CUBE with GROUPING() or GROUPING_ID()", 2),
    ("a non-equi or interval join (e.g. t.ts BETWEEN c.start_time AND c.end_time + INTERVAL '7' DAY)", 2),
    ("timestamp arithmetic with INTERVAL literals and DATE_TRUNC / DATEDIFF / TIMESTAMPDIFF", 2),
    ("sessionization: LAG over time + a gap flag + a running SUM to build session ids", 2),
    ("NOT EXISTS / EXISTS correlated subquery (anti/semi join)", 2),
    ("a scalar subquery in SELECT or WHERE", 2),
    ("PERCENTILE_APPROX or PERCENTILE / MEDIAN", 2),
    ("ROWS BETWEEN n PRECEDING AND m PRECEDING moving aggregates", 2),
    ("ROW_NUMBER-based deduplication keeping the latest row per key", 1),
    ("LAG / LEAD with offsets and defaults", 1),
    ("NTILE / PERCENT_RANK / CUME_DIST / DENSE_RANK", 1),
    ("CASE WHEN chains producing labels, with NULL handling via COALESCE / NULLIF", 1),
    ("REGEXP_EXTRACT / REGEXP_REPLACE / RLIKE on text columns", 1),
    ("string building with CONCAT_WS / LPAD / SPLIT / ARRAY_JOIN", 1),
    ("UNION ALL of two differently filtered branches", 1),
    ("a LEFT ANTI or LEFT SEMI style filter written as LEFT JOIN ... WHERE x IS NULL", 1),
    ("conditional aggregation SUM(CASE WHEN ...) with ratio math and ROUND", 1),
    ("DECIMAL casts and ROUND / FLOOR / CEIL arithmetic", 1),
    ("a final ORDER BY on several columns with NULLS LAST / DESC", 1),
]
RARE = [f for f, w in FEATURES if w == 3]

TRAIN_DOMAINS = [
    "ride-hailing trips and driver payouts", "hotel reservations and room revenue", "airline bookings and loyalty",
    "subscription streaming and churn", "online learning courses and quizzes", "hospital admissions and billing",
    "pharmacy prescriptions and refills", "insurance claims and policies", "retail banking card transactions",
    "loan applications and repayments", "payroll and timesheets", "recruiting pipeline and interviews",
    "SaaS product usage and feature flags", "customer support tickets and SLAs", "mobile game sessions and purchases",
    "food delivery orders and couriers", "grocery inventory and replenishment", "manufacturing work orders and scrap",
    "fleet telematics and maintenance", "electric vehicle charging sessions", "solar farm generation and outages",
    "telecom call detail records and plans", "real estate listings and offers", "property management rent and leases",
    "event ticketing and seating", "sports league matches and player stats", "museum visits and memberships",
    "library loans and holds", "university enrollment and grades", "clinical trial visits and adverse events",
    "agriculture harvests and soil sensors", "shipping containers and port calls", "parcel delivery scans",
    "cloud infrastructure billing and usage", "CI/CD pipeline runs and test results", "ad impressions and clicks",
    "newsletter sends and opens", "marketplace sellers and reviews", "B2B invoices and payments",
    "procurement purchase orders and vendors", "restaurant point-of-sale and tips", "gym check-ins and classes",
    "dental clinic appointments", "public parks permits and events", "public transit taps and routes",
    "bike sharing rentals and docks", "weather stations and forecasts", "smart home device events",
    "cybersecurity login events and alerts", "app store downloads and ratings", "podcast episodes and listens",
    "crowdfunding campaigns and pledges", "wine distribution and shipments", "construction projects and change orders",
]
# eval-only domains (never used for training cases)
HELDOUT_DOMAINS = [
    "airport baggage handling", "film production budgets and shoots", "space-weather satellite readings",
    "orchestra concerts and subscriptions", "ski resort lift passes", "fishing fleet catches and quotas",
    "esports tournaments and teams", "elevator maintenance and faults", "blood bank donations and transfusions",
    "theme park rides and wait times", "coffee roasting batches and cafes", "court cases and hearings",
]
# tables of eval/real_tables.py (+ themes very close to the 4 real queries): never use them
FORBIDDEN_TABLES = {"raw_events", "marketing_campaigns", "fraud_blacklist", "orders", "warehouse_inventory",
                    "blacklisted_fulfillments", "warehouse_sla", "sensor_telemetry", "calibration_registry",
                    "operational_incidents", "device_metadata", "trade_stream", "cleared_settlement_batches"}
AVOID_THEMES = ("marketing attribution of web events, e-commerce order fulfillment from warehouses, IoT sensor "
                "telemetry with calibration, crypto/stock trade execution and settlement")

STYLE_KNOBS = {
    "keywords": ["UPPERCASE SQL keywords", "lowercase SQL keywords", "mostly uppercase keywords, lowercase functions"],
    "aliases": ["short table aliases (o, li, c)", "descriptive aliases (ord, cust)", "mixed: some aliases, some none"],
    "comments": ["a -- comment above each CTE explaining its purpose", "a header block comment plus a few inline "
                 "-- comments", "sparse comments, only on tricky steps"],
    "names": ["plain snake_case table names", "warehouse-style prefixes (stg_, dim_, fct_)",
              "schema-less names with some abbreviations (txn, acct, qty)"],
}

STYLE_EXAMPLE_SQL = """WITH recent AS (
    -- Filter enrollments
    SELECT id, student_id, course_id, score, DATE_FORMAT(enrolled_at, 'MMM yyyy') AS enrolled_at_label
    FROM enrollments
    WHERE (student_id IS NOT NULL OR score <= 85.2)
)
SELECT r.*, st.student_name,
       ROUND(score - AVG(score) OVER (PARTITION BY course_id), 2) AS score_vs_course_avg
FROM recent r
LEFT JOIN students st ON st.id = r.student_id AND st.is_international = FALSE
ORDER BY score_vs_course_avg DESC NULLS LAST, id"""
STYLE_EXAMPLE_CODE = """from pyspark.sql import functions as F
from pyspark.sql.window import Window

recent = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) | (F.col("score") <= 85.2))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
    )
)

w_course = Window.partitionBy("course_id")
result = (
    recent.alias("r")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("r.student_id")) & (~F.col("st.is_international")), "left")
    .select(
        "r.*",
        "st.student_name",
        F.round(F.col("score") - F.avg("score").over(w_course), 2).alias("score_vs_course_avg"),
    )
    .orderBy(F.col("score_vs_course_avg").desc_nulls_last(), "id")
)"""

SYSTEM = f"""You write training data for a small model that converts Spark SQL to PySpark DataFrame code.
Each case is: a few small tables with sample data, one long realistic Spark SQL query over them, and an exact
PySpark DataFrame translation of that query.

The query must look like SQL a data engineer really writes at work: 90-150 lines, 4-6 CTEs, realistic table and
column names for the domain, comments, and several advanced constructs mixed together (the checklist you get).
It must be valid Spark SQL (Spark 4) and deterministic: window ORDER BYs must not have ties that change results
(add a unique key as a tie-breaker), no rand()/current_timestamp(), and the final result must have 3-60 rows.

Tables: invent 3-5 tables. Each table is given as a Spark SQL query of the form
  SELECT * FROM VALUES (...), (...) AS t(col1, col2, ...)
with 6-20 rows and typed literals (TIMESTAMP '2025-03-01 10:00:00', DATE '...', CAST(... AS DECIMAL(10,2)),
ARRAY(...), NAMED_STRUCT(...), MAP(...), NULL with CAST(NULL AS type) where the type is not obvious).
Choose rows so that every filter, join condition, window and CASE branch actually changes the result
(some rows pass and some fail each condition), and include a few NULLs where the query handles them.
Never use these table names: {", ".join(sorted(FORBIDDEN_TABLES))}.

{kr.RULES}
- House style (follow the example): imports first (`from pyspark.sql import functions as F`, and
  `from pyspark.sql.window import Window` when windows are used); one Python variable per CTE, named like the CTE;
  chained methods inside parentheses, one call per line; F.col(...) for columns; .alias(...) on derived columns;
  named Window specs for reused windows. Prefer native DataFrame functions; use F.expr only for things the
  DataFrame API cannot express (at most 2 uses, not counting INTERVAL literals).
- INTERVAL literals: write them as F.expr("INTERVAL '7' DAY") (e.g. F.col("ts") + F.expr("INTERVAL '36' HOUR")).
  Import nothing except pyspark (no datetime / timedelta).

House-style example:
```sql
{STYLE_EXAMPLE_SQL}
```
```python
{STYLE_EXAMPLE_CODE}
```

Use the check_case tool to run your case in Spark. It reports SQL errors, PySpark errors, and any difference
between the SQL result and the PySpark result. Fix the SQL, the data or the code until the check passes, then
reply with the single word DONE. You have at most 6 checks."""

TOOL = {"type": "function", "function": {
    "name": "check_case",
    "description": "Run the tables, the SQL and the PySpark translation in Spark and compare the results.",
    "parameters": {"type": "object", "properties": {
        "tables": {"type": "object", "description": "table name -> Spark SQL 'SELECT * FROM VALUES ... AS t(...)'",
                   "additionalProperties": {"type": "string"}},
        "sql": {"type": "string", "description": "the full Spark SQL query"},
        "code": {"type": "string", "description": "the complete PySpark translation; assigns `result`"},
        "order_cols": {"type": "array", "items": {"type": "string"},
                       "description": "output columns of the final ORDER BY, in order ([] if none)"},
    }, "required": ["tables", "sql", "code", "order_cols"]}}}


def make_brief(rng: random.Random, domain: str) -> tuple[str, list[str], dict]:
    k = rng.randint(8, 12)
    rare = rng.sample(RARE, 3)
    pool = [f for f, _ in FEATURES if f not in rare]
    weights = [w for f, w in FEATURES if f not in rare]
    feats = list(rare)
    while len(feats) < k:
        f = rng.choices(pool, weights)[0]
        if f not in feats:
            feats.append(f)
    rng.shuffle(feats)
    style = {k2: rng.choice(v) for k2, v in STYLE_KNOBS.items()}
    brief = (f"Domain: {domain}. (Avoid these themes: {AVOID_THEMES}.)\n"
             "Feature checklist (use all of them, naturally):\n" + "\n".join(f"- {f}" for f in feats) +
             "\nStyle: " + "; ".join(style.values()) + ".\n"
             "Write the case and check it with check_case.")
    return brief, feats, style


# ---------------------------------------------------------------------------------------------------------------
# Spark check
# ---------------------------------------------------------------------------------------------------------------
class Spark:
    def __init__(self):
        from pyspark.sql import SparkSession
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        self.root = (SparkSession.builder.master("local[4]").appName("realstyle-v19")
                     .config("spark.ui.enabled", "false")
                     .config("spark.sql.shuffle.partitions", "1")
                     .config("spark.driver.memory", "4g")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.adaptive.enabled", "false")
                     .config("spark.ui.showConsoleProgress", "false")
                     .getOrCreate())
        self.root.sparkContext.setLogLevel("OFF")
        import logging
        for name in ("SQLQueryContextLogger", "DataFrameQueryContextLogger", "py4j", "pyspark"):
            logging.getLogger(name).setLevel(logging.CRITICAL + 1)

    def check(self, args: dict) -> tuple[bool, str, dict]:
        """Returns (passed, message for Kimi, info)."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        tables, sql, code = args.get("tables"), args.get("sql"), args.get("code")
        order_cols = args.get("order_cols") or []
        if not isinstance(tables, dict) or not tables or not isinstance(sql, str) or not isinstance(code, str):
            return False, "Invalid arguments: need tables (object), sql (string), code (string), order_cols.", {}
        bad = sorted(set(map(str.lower, tables)) & FORBIDDEN_TABLES)
        if bad:
            return False, f"Forbidden table names: {bad}. Rename them.", {}
        n_lines = len([l for l in sql.strip().splitlines() if l.strip()])
        if n_lines < 80:
            return False, f"The SQL has only {n_lines} non-empty lines; write a longer, realistic query (90-150).", {}
        if kr.SQL_CALL.search(code) or "spark.table" in code:
            return False, "The code must use the df_<table> DataFrames, not spark.sql / spark.table.", {}
        bad_imports = [l.strip() for l in code.splitlines()
                       if re.match(r"\s*(import|from)\s", l) and not re.match(r"\s*from\s+pyspark[.\s]", l)]
        if bad_imports:
            return False, f"Only pyspark imports are allowed: {bad_imports[:2]}. Write INTERVALs as F.expr(\"INTERVAL ...\").", {}
        n_expr = len(re.findall(r"\bF\.expr\s*\((?!\s*[\"']\s*INTERVAL)|\.selectExpr\s*\(", code, re.I))
        if n_expr > 2:
            return False, f"The code uses F.expr/selectExpr {n_expr} times (max 2); use DataFrame functions.", {}
        problem = kr.static_check(code, sql)
        if problem:
            return False, f"Static check failed: {problem}", {}
        spark = self.root.newSession()
        ns = {"F": F, "Window": Window, "spark": spark}
        sizes = {}
        for name, tsql in tables.items():
            try:
                df = spark.sql(str(tsql).strip().rstrip(";"))
                n = df.count()
            except Exception as e:  # noqa: BLE001
                return False, f"Table {name} failed: {kr.short_error(e)}", {}
            if n < 5:
                return False, f"Table {name} has {n} rows; give it 6-20 rows.", {}
            df.createOrReplaceTempView(name)
            ns[f"df_{name}"] = df
            sizes[name] = n
        try:
            sdf = spark.sql(sql.strip().rstrip(";"))
            s_cols, s_rows = sdf.columns, rows_of(sdf)
        except Exception as e:  # noqa: BLE001
            return False, f"SQL error: {kr.short_error(e)}", {}
        if not 3 <= len(s_rows) <= 60:
            return False, f"The SQL returns {len(s_rows)} rows; adjust the data or query so it returns 3-60.", {}
        try:
            res = kr.exec_code(code, ns)
            p_cols, p_rows = res.columns, rows_of(res)
        except Exception as e:  # noqa: BLE001
            return False, f"PySpark error: {kr.short_error(e)}\nSQL output columns: {s_cols}", {}
        r = compare(s_cols, s_rows, p_cols, p_rows, order_cols)
        info = {"sql_rows": len(s_rows), "sql_lines": n_lines, "table_rows": sizes, "f_expr": n_expr}
        if not r.ok:
            return False, f"Mismatch ({r.status}): {r.detail[:600]}", info
        return True, f"PASS: SQL and PySpark agree ({len(s_rows)} rows, columns {s_cols}). Reply DONE.", info


# ---------------------------------------------------------------------------------------------------------------
# Kimi loop
# ---------------------------------------------------------------------------------------------------------------
def _add_usage(usage: dict, u) -> None:
    if not u:
        return
    g = (lambda k: u.get(k)) if isinstance(u, dict) else (lambda k: getattr(u, k, None))
    usage["input"] += g("prompt_tokens") or 0
    usage["output"] += g("completion_tokens") or 0
    d = g("prompt_tokens_details")
    dc = (d.get("cached_tokens") if isinstance(d, dict) else getattr(d, "cached_tokens", None)) if d else None
    usage["cache_read"] += dc or g("cached_tokens") or 0


def chat_turn(client, usage: dict, **kw) -> dict:
    """One streamed assistant turn -> message dict (content, reasoning_content, tool_calls). Streaming keeps bytes
    flowing while Kimi thinks, so a dead connection fails after the client's read timeout (5 min) instead of hanging
    for 30 min; the caller retries just this turn. Usage arrives in the last chunk (top level or in choices[0])."""
    content, reasoning, calls, got_usage = [], [], {}, False
    stream = client.chat.completions.create(stream=True, stream_options={"include_usage": True}, **kw)
    for ch in stream:
        u = getattr(ch, "usage", None)
        if not u and ch.choices:
            u = getattr(ch.choices[0], "usage", None) or (ch.choices[0].model_extra or {}).get("usage")
        if u and not got_usage:
            _add_usage(usage, u)
            got_usage = True
        if not ch.choices:
            continue
        d = ch.choices[0].delta
        if d is None:
            continue
        if d.content:
            content.append(d.content)
        rc = getattr(d, "reasoning_content", None) or (d.model_extra or {}).get("reasoning_content")
        if rc:
            reasoning.append(rc)
        for tc in d.tool_calls or []:
            c = calls.setdefault(tc.index, {"id": None, "type": "function", "function": {"name": "", "arguments": ""}})
            if tc.id:
                c["id"] = tc.id
            if tc.function and tc.function.name:
                c["function"]["name"] += tc.function.name
            if tc.function and tc.function.arguments:
                c["function"]["arguments"] += tc.function.arguments
    msg = {"role": "assistant", "content": "".join(content)}
    if reasoning:
        msg["reasoning_content"] = "".join(reasoning)
    if calls:
        msg["tool_calls"] = [calls[i] for i in sorted(calls)]
    return msg


class CaseError(Exception):
    def __init__(self, err: Exception, usage: dict, checks: list):
        super().__init__(f"{type(err).__name__}: {err}")
        self.usage, self.checks = usage, checks


def run_case(client, model: str, effort: str, spark: Spark, case_id: str, domain: str, brief: str,
             max_checks: int = 6, turn_retries: int = 2) -> dict:
    messages: list = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": brief}]
    usage = {"input": 0, "output": 0, "cache_read": 0}
    checks, passed = [], None
    t0 = time.time()
    for _ in range(max_checks + 3):
        for attempt in range(turn_retries + 1):
            try:
                msg = chat_turn(client, usage, model=model, messages=messages, tools=[TOOL],
                                reasoning_effort=effort, max_tokens=32768)
                break
            except Exception as e:  # noqa: BLE001  (timeouts, dropped streams, 5xx): retry this turn only
                if attempt == turn_retries:
                    raise CaseError(e, usage, checks) from e
                print(f"[{time.strftime('%H:%M:%S')}] {case_id} turn retry {attempt + 1}: {type(e).__name__}",
                      flush=True)
                time.sleep(20)
        messages.append(msg)
        if not msg.get("tool_calls"):
            break
        for tc in msg["tool_calls"]:
            if len(checks) >= max_checks:
                out = f"No checks left ({max_checks} used). Reply DONE."
            else:
                try:
                    args = json.loads(tc["function"]["arguments"])
                    ok, out, info = spark.check(args)
                except json.JSONDecodeError:
                    args, ok, out, info = {}, False, "Invalid JSON arguments.", {}
                except Exception as e:  # noqa: BLE001
                    args, ok, out, info = {}, False, f"Checker error: {kr.short_error(e)}", {}
                checks.append({"ok": ok, "msg": out[:300]})
                if ok:
                    passed = {**args, **info}
                out += f"\n[check {len(checks)} of {max_checks}]"
            messages.append({"role": "tool", "tool_call_id": tc["id"], "content": out})
        if passed and checks[-1]["ok"]:
            # one more turn lets Kimi say DONE; not needed - stop here to save tokens
            break
    return {"id": case_id, "domain": domain, "ok": passed is not None, "case": passed, "checks": checks,
            "usage": usage, "cost_usd": round(kr.cost(model, usage), 4), "seconds": round(time.time() - t0, 1)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--split", choices=["train", "eval"], default="train")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--budget", type=float, default=15.0, help="stop starting new cases above this USD spend")
    ap.add_argument("--effort", default="high", choices=["low", "high", "max"])
    ap.add_argument("--seed", type=int, default=19)
    ap.add_argument("--out", default="realstyle_v19/pilot")
    args = ap.parse_args()

    kr.load_env(REPO / ".env")
    model = os.environ.get("KIMI_MODEL") or kr.DEFAULT_MODEL
    # streamed turns: a 5 min gap between chunks = dead connection (most "30 min timeouts" were hung connections, not
    # long thinking); run_case retries that turn itself, so no silent SDK retries
    import httpx
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["KIMI_API_KEY"], max_retries=0,
                    timeout=httpx.Timeout(connect=30, read=300, write=60, pool=60),
                    base_url=os.environ.get("KIMI_BASE_URL") or kr.DEFAULT_BASE_URL)
    out = MLX / args.out
    out.mkdir(parents=True, exist_ok=True)
    # resume: passing cases and real check failures are done; cases that hit an API error are retried
    done_ids, prior_cost = set(), 0.0
    if (out / "failed.jsonl").exists():
        rows = [json.loads(l) for l in (out / "failed.jsonl").read_text().splitlines() if l.strip()]
        kept = [r for r in rows if not r.get("error")]
        (out / "failed.jsonl").write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in kept))
        print(f"retrying {len(rows) - len(kept)} cases that hit an API error", flush=True)
    for fn in ("cases.jsonl", "failed.jsonl"):
        if (out / fn).exists():
            rows = [json.loads(l) for l in (out / fn).read_text().splitlines() if l.strip()]
            done_ids |= {r["id"] for r in rows}
            prior_cost += sum(r.get("cost_usd", 0.0) for r in rows)
    rng = random.Random(args.seed + (0 if args.split == "train" else 1000))
    domains = TRAIN_DOMAINS if args.split == "train" else HELDOUT_DOMAINS
    jobs = []
    for i in range(args.n):
        domain = domains[i % len(domains)] if args.split == "eval" else rng.choice(domains)
        brief, feats, style = make_brief(rng, domain)
        cid = f"rs{args.split[0]}{i:05d}"
        if cid not in done_ids:
            jobs.append((cid, domain, brief, feats, style))
    print(f"[{time.strftime('%H:%M:%S')}] {len(jobs)} cases to run ({len(done_ids)} already done), model {model}, "
          f"effort {args.effort}, {args.workers} workers, budget ${args.budget} (spent before: ${prior_cost:.2f})", flush=True)

    spark = Spark()
    lock = threading.Lock()
    spent = {"usd": prior_cost}    # the budget covers earlier runs into the same folder too
    stats = {"ok": 0, "failed": 0}

    def work(job):
        cid, domain, brief, feats, style = job
        with lock:
            if spent["usd"] >= args.budget:
                return None
        try:
            r = run_case(client, model, args.effort, spark, cid, domain, brief)
        except Exception as e:  # noqa: BLE001  (CaseError keeps the usage of the turns that did finish)
            u = getattr(e, "usage", {}) or {}
            r = {"id": cid, "domain": domain, "ok": False, "case": None, "checks": getattr(e, "checks", []),
                 "error": str(e)[:500], "trace": traceback.format_exc()[-1500:],
                 "usage": u, "cost_usd": round(kr.cost(model, u), 4) if u else 0.0, "seconds": 0}
        r.update({"split": args.split, "features": feats, "style": style})
        with lock:
            spent["usd"] += r["cost_usd"]
            stats["ok" if r["ok"] else "failed"] += 1
            with open(out / ("cases.jsonl" if r["ok"] else "failed.jsonl"), "a", encoding="utf-8") as f:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
            print(f"[{time.strftime('%H:%M:%S')}] {cid} {'OK  ' if r['ok'] else 'FAIL'} checks={len(r['checks'])} "
                  f"${r['cost_usd']:.3f} {r['seconds']:.0f}s | total ok={stats['ok']} failed={stats['failed']} "
                  f"spent=${spent['usd']:.2f}", flush=True)
        return r

    with ThreadPoolExecutor(args.workers) as pool:
        for _ in as_completed([pool.submit(work, j) for j in jobs]):
            pass

    # report over everything in the output folder
    ok = [json.loads(l) for l in (out / "cases.jsonl").read_text().splitlines()] if (out / "cases.jsonl").exists() else []
    bad = [json.loads(l) for l in (out / "failed.jsonl").read_text().splitlines()] if (out / "failed.jsonl").exists() else []
    allr = ok + bad
    cost_total = sum(r["cost_usd"] for r in allr)
    report = {"cases": len(allr), "ok": len(ok), "pass_rate": round(len(ok) / max(1, len(allr)), 3),
              "cost_usd": round(cost_total, 2), "cost_per_ok_usd": round(cost_total / max(1, len(ok)), 3),
              "median_seconds": sorted(r["seconds"] for r in allr)[len(allr) // 2] if allr else 0,
              "mean_checks_ok": round(sum(len(r["checks"]) for r in ok) / max(1, len(ok)), 2),
              "sql_lines_median": sorted(r["case"]["sql_lines"] for r in ok)[len(ok) // 2] if ok else 0,
              "errors": sum(1 for r in bad if r.get("error"))}
    (out / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
