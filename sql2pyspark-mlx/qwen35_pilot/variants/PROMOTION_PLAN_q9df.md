# Promotion plan: sql2pyspark:q9df-q4 → q9df3-q4 (2026-11-24 draft; outcome 2026-11-25)

## Outcome (2026-11-25): q9df3-q4 PROMOTED, 19:20:55 Bangkok time

The owner chose option A (fix round q9f2, 11-25 06:45), then the smaller fix weight (12:25), and pre-approved the
promotion (~18:30) if the full rule and the app test passed. **q9df3-q4 = 0.5 q9d + 0.25 q9f + 0.25 q9f2** (fused,
weight-averaged, Q4_K_M; [`step_q9df3.sh`](step_q9df3.sh)):

- Full rule vs. v18a-q4: hold-out 61 / 61, v4 21 / 20, fixed 147 / 145, complex 93 / 88, complex-shift 131 / 126,
  schema-shift 186 / 186, v11–v14 295 / 247 / 150 / 200 vs. 281 / 232 / 148 / 195, name slips 32 / 66 (failures 130 / 179). **Pass.**
- Loop probe (`eval/star_probe.jsonl`, 30): 28 correct, **0 loops** (q9df 18 / 6 loops; v18a 27 / 0). q9df3b (1/8 of the fix) kept 1 loop.
- App test ([`apptest/`](apptest/), Streamlit AppTest, Kimi off): 7/7 incl. 4 `SELECT *` + `[NOT] IN` / `[NOT] EXISTS`, 2-4 s
  each, with q9df3-q4 **and** with v18a-q4; the patched app also served on a separate port (8512, health ok).
- `app.py` change (live): `"think": False` in the Ollama request + `"num_predict": 4096` in `generation_options()`.
  Of q9df3's 74 real-style replies 6 reach 4,096 tokens: 3 are loops, 3 long unfinished answers; with 8,192 one of
  them still did not finish (317 s), so the cap stays at 4,096 (a loop now ends after ~2.5 min at 26 tok/s).
- Steps done: `app.py` changed and the app restarted (`pkill -f "streamlit run app.py --server.port 8502"`, KeepAlive)
  at 19:20:46, then `ollama cp sql2pyspark:q9df3-q4 sql2pyspark:latest` (`94c353738904`) at 19:20:55; AppTest on the
  live `app.py` with `sql2pyspark` 7/7; health ok; anonymous request to the site → 302 to Cloudflare Access.
  Root GGUFs stay v18a (owner).
- **Rollback:** `ollama cp sql2pyspark:v18a-q4 sql2pyspark:latest` (v18a works with the new `app.py`; to restore the
  old `app.py` too: `git checkout <commit before> -- app.py` and restart the app as above).

The draft below is the 2026-11-24 state, kept as the record.

---

## Draft (2026-11-24 ~20:30 Bangkok time)

Status: **q9df-q4 passes the promotion rule on every eval set, but the local app test found a blocker**:
it loops on `SELECT *` combined with an `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` subquery. The owner decides. Nothing
has been changed on the site, `sql2pyspark:latest`, v18a-q4, v15-q4 or the root GGUFs.

### What q9df is
`q9df-q4` = 50 / 50 weight average of fused **q9d** (Qwen3.5-9B QLoRA, polish round, 11-22) and fused **q9f**
(q9d + one ORPO preference round on 234 of its own Spark-checked mistakes, 11-24). Q4_K_M,
`variants/gguf/q9df-q4.gguf`, Modelfile `variants/Modelfile.q9df-q4` (= Modelfile.q35 template).
q9f alone reached hold-out 61 but looped on 5 fixed-set queries (fixed 135); averaging with q9d removed those loops.

### Rule check (vs v18a-q4, think:false for q9 models)
| Set | v18a-q4 | q9d-q4 | q9df-q4 | Rule |
|---|---|---|---|---|
| Hold-out (62) | 61 | 59 | **61** | ✅ ≥ 61 |
| Hold-out v4 (25) | 20 | 22 | **21** | ✅ ≥ 20 |
| Fixed (150) | 145 | 147 | **147** | ✅ ≥ 145 |
| Complex (100) | 88 | 95 | **92** | ✅ ≥ 88 |
| Complex-shift (150) | 126 | 130 | **131** | ✅ ≥ 126 |
| Schema-shift (200) | 186 | 185 | **191** | ✅ no drop |
| v11 / v12 / v13 / v14 held-out | 281 / 232 / 148 / 195 | 294 / 245 / 150 / 199 | **295 / 248 / 150 / 199** | ✅ no drop |
| IN / NOT IN (50) | 50 | 50 | 49 | (not in rule) |
| Name slips (all sets incl. real-style) | 66 | 29 | **31** | ✅ ≤ 53 |
| Failures, all sets | 179 | 61 (no real-style) | 126 (52 + 74 real-style) | |
| Harness 21 (strict) | 5 | 5 | **6** | |
| Real-style 74 | 0 | — | 0 | |
| Dev set (100, selection only) | 95 | 93 | 95 | |

### Local app test (patched copy of app.py, Streamlit AppTest, Kimi off, no site change)
1. **The app must send `"think": false`.** app.py sends no think flag; Ollama reports the `thinking` capability for the
   Qwen3.5 GGUFs, and without the flag q9df did not answer a simple GROUP BY within 600 s. With `"think": false`:
   no `<think>` leak, normal answers. v18a-q4 accepts the flag (ignored), so the one-line change is safe for both.
   Patched copy: scratchpad `apptest/app.py` (diff: one line after `"stream": False,`).
2. AppTest with q9df: 3 of 4 conversions OK in 4-8 s with "Basic checks passed"; the 4th
   (`SELECT * FROM orders o WHERE o.id NOT IN (SELECT ...)`) **looped to the token limit**.
3. **Blocker, reproduced at temperature 0 and 0.2** (num_predict 400-2048):

   | Query shape | v18a-q4 | q9e-q4 | q9de-q4 | q9d-q4 | q9df-q4 |
   |---|---|---|---|---|---|
   | `SELECT * ... WHERE id NOT IN (SELECT ...)` | ok | ok | ok | LOOP | **LOOP** |
   | `SELECT * ... WHERE NOT EXISTS (SELECT ...)` | ok | ok | ok | | **LOOP** |
   | `SELECT * ... WHERE EXISTS (SELECT ...)` | | ok | ok | | **LOOP** |
   | `SELECT * ... WHERE x IN (SELECT ...)` | | ok | ok | | **LOOP** |
   | `SELECT o.* ... NOT IN (SELECT ...)`, `SELECT * ... WHERE city = ...` | ok | | | | ok |

   It comes from q9d (same loop), not from the ORPO round; `repeat_last_n 2048` does not stop it. The training data
   and the 6,000-query self-play pool contain **0** queries of this shape, which is why no eval saw it.
   **Site risk:** app.py sets no `num_predict`, so a looping request generates up to the 32k context (~15+ min)
   while Ollama runs one request at a time (`OLLAMA_NUM_PARALLEL=1`) — every other visitor waits behind it.

### Options (owner decides)
- **A. Fix, then re-check (recommended, ~1 day, no site change):** build ~300-500 Spark-verified pairs of
  `SELECT *` / `SELECT t.*` with IN / NOT IN / EXISTS / NOT EXISTS / scalar subqueries on the Checker tables; collect
  q9df's replies (loops become "rejected"); second short ORPO round from the q9f adapter on these + the 234 pairs; merge
  again with q9d (q9df2); quick evals + this loop probe + full evals; app test again.
- **B. Promote q9df with app-side guards:** app.py `"think": false` + `num_predict` cap (e.g. 3072) + on a cut-off reply
  retry once with v18a-q4. Changes the live app code; loops still cost ~2 min per affected request.
- **C. Keep v18a-q4** (no promotion), keep all q9 files.

### Promotion steps (only after the owner's OK; for option A's result or B)
1. app.py: add `"think": False` (and for B the `num_predict` cap + fallback); run the app test on a separate port again.
2. `ollama show sql2pyspark:v18a-q4` exists → rollback tag kept. `ollama cp sql2pyspark:<tag> sql2pyspark:latest`
   (= `make promote TAG=<tag> PREV=v18a-q4`).
3. `sudo launchctl kickstart -k system/com.praponb.sql2pyspark` (app code change), then check
   https://sql2pyspark.praponb.com with 3 queries incl. one `SELECT * ... NOT IN (...)`.
4. Root GGUFs (`sql2pyspark-q4.gguf`) → the promoted GGUF only if the owner wants them to follow `latest`.
5. Rollback: `ollama cp sql2pyspark:v18a-q4 sql2pyspark:latest` (+ revert app.py and kickstart).
6. One commit + docs (results, q9d→q9f→q9df method, this blocker), then PR — after the owner's decision.
