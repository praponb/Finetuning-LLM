"""Step B pilot: harness-style SQL -> PySpark TRAINING pairs written by Kimi K3.1 Pro, verified type-strict in Spark.

Same tool loop as realstyle_v19/generate.py, aimed at the gaps the harness suites show (nested types, higher-order
functions, generators, set-operation / NULL traps, time windows, gap filling, stats, decimals):
  - medium-length queries (25-80 SQL lines; SQL + code <= 8,000 chars, so a pair fits the training sequence length)
  - check = sql2pyspark_harness/compare_harness.compare_one (same column names AND Spark types, row multiset, every
    value computed) + verify_pairs.compare with order_cols (ORDER BY respected)
  - never the 13 tables of sql2pyspark_sql_suite (the harness v2 test set) or the suite's e-commerce domain
  - HARD budget: a case starts only if spent + reservations of running cases + one more reservation <= budget - margin;
    a case stops once its own cost passes --case-cap; a turn that dies without usage is billed at its worst case.

Run from sql2pyspark-mlx/ (reads KIMI_API_KEY from ../.env):
    .venv/bin/python harness_v20/generate.py --n 60 --workers 4 --budget 10 --out harness_v20/pilot
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
sys.path.insert(0, str(REPO / "sql2pyspark_harness"))

import compare_harness as ch  # noqa: E402
import kimi_repair as kr  # noqa: E402
from realstyle_v19 import generate as rs  # noqa: E402  (chat_turn, CaseError, style example, domains)
from verify_pairs import compare, rows_of  # noqa: E402

# ---------------------------------------------------------------------------------------------------------------
# What to ask for: (feature, weight). Weight 3 = no / almost no pairs in the training mix and failing on the harness.
# ---------------------------------------------------------------------------------------------------------------
FEATURES = [
    ("ARRAY<STRUCT<...>> flattened with LATERAL VIEW posexplode / explode_outer / inline_outer, struct fields read "
     "from the generator alias", 3),
    ("MAP columns: element_at / map_keys / map_values / map_filter / transform_values / transform_keys / map_concat "
     "or explode(map) into key/value rows", 3),
    ("a JSON string column parsed with from_json (explicit DDL schema incl. a nested STRUCT) and get_json_object", 3),
    ("higher-order functions with index-aware lambdas: transform(arr, (x, i) -> ...), filter, aggregate/reduce with "
     "a finish lambda, exists / forall", 3),
    ("array_sort with a comparator lambda, arrays_zip, slice, element_at with a negative index, array_position, "
     "try_element_at", 3),
    ("max_by / min_by (optionally over a struct), bool_or / bool_and, count_if", 3),
    ("LEFT SEMI JOIN and LEFT ANTI JOIN written with that SQL syntax", 3),
    ("NOT IN against a subquery where NULLs matter, next to the NOT EXISTS form that behaves differently", 3),
    ("INTERSECT ALL / EXCEPT ALL (multiset) or INTERSECT / EXCEPT DISTINCT", 3),
    ("a window frame RANGE BETWEEN INTERVAL '...' PRECEDING AND CURRENT ROW on a timestamp or date", 3),
    ("stack() to unpivot, or a SQL UNPIVOT of two value columns at once keeping NULLs (INCLUDE NULLS)", 3),
    ("PIVOT with two aggregates or on a tuple of columns", 3),
    ("time bucketing with window(ts, '1 hour'[, slide]) or session_window(ts, gap)", 3),
    ("a date spine built with sequence(...) + explode, then forward fill with last(... , true) / IGNORE NULLS", 3),
    ("gaps-and-islands: ROW_NUMBER difference or date_sub(d, rank) to group consecutive rows into streaks", 3),
    ("as-of / SCD2 join: half-open validity interval (valid_from <= ts AND ts < valid_to) with LEAD(.., 1, default) "
     "and a null-safe <=> or IS DISTINCT FROM change test", 3),
    ("percentile_cont / percentile_disc WITHIN GROUP, median, or percentile_approx with an accuracy argument", 2),
    ("regr_slope / corr / covar_samp / stddev_samp / skewness statistics per group", 2),
    ("width_bucket histogram, or a window over an aggregate in the same SELECT (SUM(COUNT(*)) OVER ())", 2),
    ("DECIMAL arithmetic with explicit precision, integer division (div), try_divide / try_cast and ROUND modes; "
     "result column types must be exact", 2),
    ("three-valued logic: NULL-aware CASE, COALESCE / NVL2 / NULLIF, comparisons with NULL, count(col) vs count(*)", 2),
    ("regexp_extract_all / regexp_replace with groups, str_to_map, split_part, parse_url, LIKE ANY / ILIKE", 2),
    ("GROUPING SETS / ROLLUP / CUBE with grouping_id() and an aggregate FILTER (WHERE ...) clause", 2),
    ("GROUP BY ALL, or a correlated scalar subquery in SELECT or HAVING", 2),
    ("a tuple IN subquery: (a, b) IN (SELECT x, y FROM ...)", 2),
    ("a broadcast join hint /*+ BROADCAST(t) */ on a small dimension table", 1),
    ("sessionization: LAG over time + a gap flag + a running SUM for session ids", 1),
    ("NTILE / PERCENT_RANK / CUME_DIST / DENSE_RANK", 1),
    ("timestamp arithmetic: make_timestamp, date_trunc, timestampdiff, next_day, from_utc_timestamp", 1),
    ("collect_list / collect_set with array_sort and array_join, struct building with named_struct", 1),
]
RARE = [f for f, w in FEATURES if w == 3]

# the harness v2 test set (sql2pyspark_sql_suite): never train on its tables or its e-commerce theme
SUITE_TABLES = {"customers", "products", "orders", "order_items", "payments", "fx_rates", "web_events", "promotions",
                "inventory_movements", "tier_history", "category_tree", "support_tickets", "price_history"}
FORBIDDEN_TABLES = SUITE_TABLES | rs.FORBIDDEN_TABLES
AVOID_THEMES = (rs.AVOID_THEMES + ", e-commerce customers/orders/order items/payments, web clickstream events, "
                "inventory movements / FIFO stock, customer loyalty tiers, support tickets, product category trees, "
                "currency FX rates")
SUITE_LIKE = {"customer support tickets and SLAs", "grocery inventory and replenishment", "marketplace sellers and reviews"}
DOMAINS = [d for d in rs.TRAIN_DOMAINS if d not in SUITE_LIKE]

SYSTEM = f"""You write training data for a small model that converts Spark SQL to PySpark DataFrame code.
Each case is: a few small tables with sample data, one realistic Spark SQL query over them, and an exact PySpark
DataFrame translation of that query.

The query must look like SQL a data engineer really writes: 25-80 lines, 1-4 CTEs, realistic table and column
names for the domain, a few comments, and the advanced constructs of the checklist you get, used naturally.
Keep it compact: the SQL plus the PySpark code together must stay under 8,000 characters.
It must be valid Spark SQL (Spark 4.2, ANSI mode on) and deterministic: window ORDER BYs must not have ties that
change results (add a unique key as a tie-breaker), no rand()/current_timestamp()/uuid(), and the final result must
have 3-40 rows. Do not use WITH RECURSIVE, SQL pipe syntax (|>), LATERAL subqueries or VARIANT.

Tables: invent 2-4 tables. Each table is given as a Spark SQL query of the form
  SELECT * FROM VALUES (...), (...) AS t(col1, col2, ...)
with 6-20 rows and typed literals (TIMESTAMP '2025-03-01 10:00:00', DATE '...', CAST(... AS DECIMAL(10,2)),
ARRAY(...), NAMED_STRUCT(...), MAP(...), JSON strings, CAST(NULL AS type) where the type is not obvious).
Choose rows so that every filter, join condition, window, generator and CASE branch actually changes the result,
and include NULLs / empty arrays where the query handles them.
Never use these table names: {", ".join(sorted(FORBIDDEN_TABLES))}.
Avoid these themes: {AVOID_THEMES}.

The check is TYPE-STRICT: the PySpark result must have the same column names, in the same order, with exactly the
same Spark data types as the SQL result (DECIMAL(p,s) vs DOUBLE, INT vs BIGINT, ARRAY / STRUCT / MAP element types
all count), and the same rows. Cast in the PySpark code where needed so the types match.

{kr.RULES}
- House style (follow the example): imports first (`from pyspark.sql import functions as F`, and
  `from pyspark.sql.window import Window` when windows are used); one Python variable per CTE, named like the CTE;
  chained methods inside parentheses, one call per line; F.col(...) for columns; .alias(...) on derived columns;
  named Window specs for reused windows. Prefer native DataFrame functions (F.transform, F.filter, F.aggregate,
  F.max_by, F.posexplode_outer, F.inline_outer, F.session_window, F.window, .join(..., "left_semi"/"left_anti"),
  .intersectAll, .exceptAll, .unpivot, .groupBy().pivot(), Window.rangeBetween ...); use F.expr only for things the
  DataFrame API cannot express (at most 2 uses, not counting INTERVAL literals).
- INTERVAL literals: write them as F.expr("INTERVAL '7' DAY"). Import nothing except pyspark.

House-style example:
```sql
{rs.STYLE_EXAMPLE_SQL}
```
```python
{rs.STYLE_EXAMPLE_CODE}
```

Use the check_case tool to run your case in Spark. It reports SQL errors, PySpark errors, schema (name / type)
differences and row differences. Fix the SQL, the data or the code until the check passes, then reply with the
single word DONE. You have at most 5 checks."""

TOOL = rs.TOOL


def make_brief(rng: random.Random, domain: str) -> tuple[str, list[str], dict]:
    k = rng.randint(3, 5)
    feats = rng.sample(RARE, 2)
    pool = [f for f, _ in FEATURES if f not in feats]
    weights = [w for f, w in FEATURES if f not in feats]
    while len(feats) < k:
        f = rng.choices(pool, weights)[0]
        if f not in feats:
            feats.append(f)
    rng.shuffle(feats)
    style = {k2: rng.choice(v) for k2, v in rs.STYLE_KNOBS.items()}
    brief = (f"Domain: {domain}.\nFeature checklist (use all of them, naturally):\n" +
             "\n".join(f"- {f}" for f in feats) + "\nStyle: " + "; ".join(style.values()) + ".\n"
             "Write the case and check it with check_case.")
    return brief, feats, style


def where(e: BaseException, code: str) -> str:
    """' (at code line N: <line>)' for the deepest traceback frame inside the exec'd snippet, else ''."""
    tb, line = e.__traceback__, None
    while tb is not None:
        if tb.tb_frame.f_code.co_filename == "<pyspark>":
            line = tb.tb_lineno
        tb = tb.tb_next
    if not line:
        return ""
    lines = code.splitlines()
    return f" (at code line {line}: {lines[line - 1].strip()[:200]})" if 0 < line <= len(lines) else ""


# ---------------------------------------------------------------------------------------------------------------
# Spark check (type-strict)
# ---------------------------------------------------------------------------------------------------------------
class Spark(rs.Spark):
    def check(self, args: dict) -> tuple[bool, str, dict]:
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
        if not 25 <= n_lines <= 85:
            return False, f"The SQL has {n_lines} non-empty lines; it must have 25-80.", {}
        if len(sql) + len(code) > 8000:
            return False, f"SQL + code are {len(sql) + len(code)} chars; keep them under 8,000 together.", {}
        if kr.SQL_CALL.search(code) or "spark.table" in code:
            return False, "The code must use the df_<table> DataFrames, not spark.sql / spark.table.", {}
        bad_imports = [l.strip() for l in code.splitlines()
                       if re.match(r"\s*(import|from)\s", l) and not re.match(r"\s*from\s+pyspark[.\s]", l)]
        if bad_imports:
            return False, f"Only pyspark imports are allowed: {bad_imports[:2]}.", {}
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
        sql_clean = sql.strip().rstrip(";")
        try:
            sdf = spark.sql(sql_clean)
            s_cols, s_rows = sdf.columns, rows_of(sdf)
        except Exception as e:  # noqa: BLE001
            return False, f"SQL error: {kr.short_error(e)}", {}
        if not 3 <= len(s_rows) <= 40:
            return False, f"The SQL returns {len(s_rows)} rows; adjust the data or query so it returns 3-40.", {}

        try:      # build once here so a Python-side error reports the code line
            kr.exec_code(code, dict(ns))
        except Exception as e:  # noqa: BLE001
            return False, f"PySpark error: {kr.short_error(e)}{where(e, code)}\nSQL output columns: {s_cols}", {}

        def build(_spark):
            return kr.exec_code(code, dict(ns))
        r = ch.compare_one(spark, ch.Statement("case", sql_clean), build, ch.Options())
        info = {"sql_rows": len(s_rows), "sql_lines": n_lines, "table_rows": sizes, "f_expr": n_expr,
                "chars": len(sql) + len(code)}
        if r.status != ch.PASS:
            msg = r.message[:700]
            if r.only_in_sql or r.only_in_pyspark:
                msg += f"\nonly in SQL: {r.only_in_sql[:3]}\nonly in PySpark: {r.only_in_pyspark[:3]}"
            return False, f"{r.status}: {msg[:1200]}\nSQL output columns: {s_cols}", info
        try:      # order (the strict check compares multisets only)
            res = kr.exec_code(code, dict(ns))
            lr = compare(s_cols, s_rows, res.columns, rows_of(res), order_cols)
        except Exception as e:  # noqa: BLE001
            return False, f"PySpark error: {kr.short_error(e)}", info
        if not lr.ok:
            return False, f"Mismatch ({lr.status}): {lr.detail[:600]}", info
        return True, f"PASS: SQL and PySpark agree, types included ({len(s_rows)} rows). Reply DONE.", info


# ---------------------------------------------------------------------------------------------------------------
# Kimi loop with a per-case cost cap
# ---------------------------------------------------------------------------------------------------------------
MAX_TOKENS = 24576


def turn_worst_usd(model: str, n_input: int) -> float:
    """Worst-case cost of one turn: all input uncached + MAX_TOKENS output."""
    return kr.cost(model, {"input": n_input, "cache_read": 0, "output": MAX_TOKENS})


def run_case(client, model, effort, spark, case_id, domain, brief, case_cap, max_checks=5, turn_retries=2):
    messages: list = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": brief}]
    usage = {"input": 0, "output": 0, "cache_read": 0}
    unlogged = [0.0]                       # worst-case USD for turns that died without reporting usage
    checks, passed = [], None
    t0 = time.time()
    stop = ""
    for _ in range(max_checks + 2):
        n_in = sum(len(json.dumps(m, ensure_ascii=False)) for m in messages) // 3
        for attempt in range(turn_retries + 1):
            if attempt and kr.cost(model, usage) + unlogged[0] >= case_cap:   # no retries past the cap
                err = rs.CaseError(RuntimeError(f"case cap ${case_cap} reached during retries"), usage, checks)
                err.unlogged = unlogged[0]
                raise err
            before = dict(usage)
            try:
                msg = rs.chat_turn(client, usage, model=model, messages=messages, tools=[TOOL],
                                   reasoning_effort=effort, max_tokens=MAX_TOKENS)
                break
            except Exception as e:  # noqa: BLE001
                if usage == before:          # no usage came back: bill the worst case
                    unlogged[0] += turn_worst_usd(model, n_in)
                if attempt == turn_retries:
                    err = rs.CaseError(e, usage, checks)
                    err.unlogged = unlogged[0]
                    raise err from e
                print(f"[{time.strftime('%H:%M:%S')}] {case_id} turn retry {attempt + 1}: {type(e).__name__}", flush=True)
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
            break
        if kr.cost(model, usage) + unlogged[0] >= case_cap:
            stop = f"case cap ${case_cap} reached"
            break
    cost = kr.cost(model, usage) + unlogged[0]
    return {"id": case_id, "domain": domain, "ok": passed is not None, "case": passed, "checks": checks,
            "usage": usage, "unlogged_usd": round(unlogged[0], 4), "cost_usd": round(cost, 4),
            "stop": stop, "seconds": round(time.time() - t0, 1)}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--n", type=int, default=60)
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--budget", type=float, default=10.0, help="hard USD limit for this output folder")
    ap.add_argument("--margin", type=float, default=0.5, help="kept free below the budget")
    ap.add_argument("--case-cap", type=float, default=0.45, help="stop a case once its cost passes this")
    ap.add_argument("--effort", default="high", choices=["low", "high", "max"])
    ap.add_argument("--seed", type=int, default=20)
    ap.add_argument("--out", default="harness_v20/pilot")
    args = ap.parse_args()

    kr.load_env(REPO / ".env")
    model = os.environ.get("KIMI_MODEL") or kr.DEFAULT_MODEL
    import httpx
    from openai import OpenAI
    client = OpenAI(api_key=os.environ["KIMI_API_KEY"], max_retries=0,
                    timeout=httpx.Timeout(connect=30, read=300, write=60, pool=60),
                    base_url=os.environ.get("KIMI_BASE_URL") or kr.DEFAULT_BASE_URL)
    out = MLX / args.out
    out.mkdir(parents=True, exist_ok=True)
    done_ids, prior_cost = set(), 0.0
    for fn in ("cases.jsonl", "failed.jsonl"):
        if (out / fn).exists():
            rows = [json.loads(l) for l in (out / fn).read_text().splitlines() if l.strip()]
            done_ids |= {r["id"] for r in rows}
            prior_cost += sum(r.get("cost_usd", 0.0) for r in rows)
    # one case can cost at most: case cap + one more worst-case turn (<= ~120k input tokens late in a case)
    reserve = args.case_cap + turn_worst_usd(model, 120000)
    limit = args.budget - args.margin
    rng = random.Random(args.seed)
    jobs = []
    for i in range(args.n):
        domain = rng.choice(DOMAINS)
        brief, feats, style = make_brief(rng, domain)
        cid = f"hv{i:05d}"
        if cid not in done_ids:
            jobs.append((cid, domain, brief, feats, style))
    print(f"[{time.strftime('%H:%M:%S')}] {len(jobs)} cases queued, model {model}, effort {args.effort}, "
          f"{args.workers} workers, budget ${args.budget} hard (start only if spent + reservations + ${reserve:.2f} "
          f"<= ${limit:.2f}), case cap ${args.case_cap}, spent before ${prior_cost:.2f}", flush=True)

    spark = Spark()
    lock = threading.Lock()
    state = {"spent": prior_cost, "reserved": 0.0, "ok": 0, "failed": 0, "skipped": 0}

    def work(job):
        cid, domain, brief, feats, style = job
        while True:   # wait while running cases hold the room; skip only when nothing runs and it still won't fit
            with lock:
                if state["spent"] + state["reserved"] + reserve <= limit:
                    state["reserved"] += reserve
                    break
                if state["reserved"] < 1e-9:
                    state["skipped"] += 1
                    return None
            time.sleep(15)
        try:
            r = run_case(client, model, args.effort, spark, cid, domain, brief, args.case_cap)
        except Exception as e:  # noqa: BLE001
            u = getattr(e, "usage", {}) or {}
            extra = getattr(e, "unlogged", 0.0)
            r = {"id": cid, "domain": domain, "ok": False, "case": None, "checks": getattr(e, "checks", []),
                 "error": str(e)[:500], "trace": traceback.format_exc()[-1500:], "usage": u,
                 "unlogged_usd": round(extra, 4),
                 "cost_usd": round((kr.cost(model, u) if u else 0.0) + extra, 4), "seconds": 0}
        r.update({"features": feats, "style": style})
        with lock:
            state["reserved"] -= reserve
            state["spent"] += r["cost_usd"]
            state["ok" if r["ok"] else "failed"] += 1
            with open(out / ("cases.jsonl" if r["ok"] else "failed.jsonl"), "a", encoding="utf-8") as f:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
            print(f"[{time.strftime('%H:%M:%S')}] {cid} {'OK  ' if r['ok'] else 'FAIL'} checks={len(r['checks'])} "
                  f"${r['cost_usd']:.3f} {r['seconds']:.0f}s {r.get('stop', '')} | ok={state['ok']} "
                  f"failed={state['failed']} spent=${state['spent']:.2f}", flush=True)
        return r

    with ThreadPoolExecutor(args.workers) as pool:
        for _ in as_completed([pool.submit(work, j) for j in jobs]):
            pass

    ok = [json.loads(l) for l in (out / "cases.jsonl").read_text().splitlines()] if (out / "cases.jsonl").exists() else []
    bad = [json.loads(l) for l in (out / "failed.jsonl").read_text().splitlines()] if (out / "failed.jsonl").exists() else []
    allr = ok + bad
    cost_total = sum(r["cost_usd"] for r in allr)
    report = {"cases": len(allr), "ok": len(ok), "pass_rate": round(len(ok) / max(1, len(allr)), 3),
              "cost_usd": round(cost_total, 2), "cost_per_ok_usd": round(cost_total / max(1, len(ok)), 3),
              "unlogged_usd": round(sum(r.get("unlogged_usd", 0) for r in allr), 2),
              "median_seconds": sorted(r["seconds"] for r in allr)[len(allr) // 2] if allr else 0,
              "mean_checks_ok": round(sum(len(r["checks"]) for r in ok) / max(1, len(ok)), 2),
              "sql_lines_median": sorted(r["case"]["sql_lines"] for r in ok)[len(ok) // 2] if ok else 0,
              "chars_median": sorted(r["case"]["chars"] for r in ok)[len(ok) // 2] if ok else 0,
              "skipped_for_budget": state["skipped"], "errors": sum(1 for r in bad if r.get("error"))}
    (out / "report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
