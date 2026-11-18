# realstyle_v19 — real-style test set built with Kimi K3.1 Pro (2026-11-15)

The 4 real-world queries are 2,400-2,700 tokens with 4-6 CTEs and many rare constructs (MAP / STRUCT / JSON,
`COLLECT_LIST(DISTINCT)`, `RANGE BETWEEN`, `LATERAL VIEW`), and every model scores 0/4 on them. Option C ("v19") was
to have Kimi write long real-style pairs and verify them in Spark. The 100-case pilot ran; the full build (~$200-300)
was not started, and on 2026-11-15 the owner decided to use the pilot pairs as an extra **test** set instead of
training data.

- [`generate.py`](generate.py): a Kimi K3.1 Pro tool loop that writes a long SQL query + house-style PySpark over its
  own small tables and must pass `check_case` (both run in Spark with equal non-empty results of 3-60 rows, ≥ 80 SQL
  lines, no real-query table names, `F.expr` ≤ 2). Streams each turn, retries a stalled turn, and keeps a cost cap.
- `pilot/cases.jsonl`: **74 verified pairs** (of 100 cases; ~$16.8). `pilot/failed.jsonl`, `pilot/report.json`,
  `pilot.log`; `smoke/` = the first 2-case smoke test.
- [`eval_realstyle.py`](eval_realstyle.py): scores an Ollama tag on the 74 pairs (each case has its own tables, so
  `eval/evaluate.py --tables` does not fit) → `eval/results/realstyle/<tag>__realstyle.json`.

| Model | syntax OK | runs | match |
| --- | --- | --- | --- |
| v18a-q4 (`latest`) | 64% | 0 | **0/74** |
| q35moe-v1-q4 / -v2-q4 | 54% / 39% | 0 | 0/74 |
| q9a-q4 | 32% | 0 | 0/74 |

No model gets a single pair right: the answers break on syntax, unresolved columns and made-up `F.` functions.
