# SQL → PySpark Fine-Tuning: Documentation Index

Welcome to the technical documentation for the **SQL → PySpark Fine-Tuning Pipeline**. This project implements an end-to-end, local machine learning and data engineering workflow on Apple Silicon to fine-tune **Qwen2.5-Coder** (v1–v5: 3B; v6–v10: 7B) for translating Spark SQL queries into idiomatic PySpark DataFrame code.

**Status (2026-11-12):**

- **Models in use:** `sql2pyspark:latest` is **v15-q4 on the Mac** (since 2026-10-09 05:50, `make promote TAG=v15-q4 PREV=v12-q4`; v12-q4 2026-10-03 06:05 → 10-09, v11-q4 2026-10-02 → 10-03) and was on the Ubuntu server 2026-10-09 06:29 → 11-08 (v12-q4 2026-10-03 16:48 → 10-09 and v9-q4 before); since 2026-11-08 the Mac mini serves the site, and the server copy was removed on 2026-11-14. **v12-q4** (2026-10-03): v11 continued on `targeted_v12` (`OR` / mixed `AND`-`OR` conditions + replay) fixes v11's `OR` slip (fixed-test `OR` queries 11/12 vs. 9, held-out `or_where` 97/100 vs. 79) and keeps the subquery gains (287/300 and 230/250 held-out targeted pairs vs. 283 / 169); 59 hold-out, 21 v4 hold-out, 143 fixed test (classic 95/97), 83 complex test; see [Evaluation](04-evaluation-and-benchmarking.md#-v12-results-2026-10-03).
- **v13 (2026-10-03) — regression, not used:** v12 continued on `targeted_v13` (2,000 uncorrelated scalar-subquery pairs + replay); v13-q4 vs. v12-q4: 150 / 123 of 150 held-out scalar-subquery pairs, but hold-out 52 / 59 (`df_patents` for `df_patients` in 7 answers), fixed test 134 / 143 (classic `subquery` family 6 / 15 of 17: correlated scalar subqueries now written as a separate aggregate), v4 hold-out 19 / 21, complex 79 / 83; the Q8_0 build is no better (49/62). v12-q4 stays `latest` on the Mac; see [Evaluation](04-evaluation-and-benchmarking.md#-v13-results-2026-10-03--regression-not-used).
- **v15 (2026-10-08) — `latest` on the Mac and the server since 2026-10-09:** v14 continued on `schema_v15` (10,000 pairs of the existing query forms on 221 synthetic tables in 35 domains with names never seen, + replay = 25,008). v15-q4 vs. v12-q4: hold-out **60** / 59, classic **97** / 95 (`subquery` 17/17), fixed test **145** / 143, complex **89** / 83, schema-shift (5 domains never trained on) **189** / 155, failures with an unresolved name **42** / 93 (v14-q4 57); but v4 hold-out 20 / **21** and v11 held-out 278 / **287**. See [Evaluation](04-evaluation-and-benchmarking.md#-v15-results-2026-10-08).
- **v17 (2026-11-08) — not promoted:** the first training run on the Mac mini M6 (32 GB; ~3 h vs. ~10 h on the M2): v15 continued on `complex_v17` (v15's full 25,008 mix + 4,000 complex_v16 pairs). v17-q4 vs. v15-q4: hold-out **61** / 60, v4 **21** / 20, complex schema-shift **116** / 110, failures **110** / 121; but fixed 142 / **145**, complex 85 / **89** → the rule fails; `latest` stays v15-q4. See [Evaluation](04-evaluation-and-benchmarking.md#-v17-results-2026-11-08--not-promoted).
- **Qwen3.5-9B (2026-11-09 → 11-12) — stopped:** after the 2026-10-09 pilot, 8 runs on the Mac mini (longer training, name-copy data, a 16-layer rank-32 adapter, a complex-focused round). Best = run 8 (`sql2pyspark:q35m6g-q4`): hold-out 59, v4 22, fixed 143, complex 82, complex-shift 111 — 7 behind v15-q4 on complex. Qwen3.8 (27B and up) was too big for long samples. See [qwen35_pilot](../sql2pyspark-mlx/qwen35_pilot/README.md).
- **v16 (2026-10-09) — not promoted:** v15 continued on `complex_v16` (10,000 complex pairs — pipelines up to 9 stages, many-window queries, set operations, gaps-and-islands, grouping sets, pivot / unpivot — on the synthetic tables, + replay = 21,008; 3,600 micro-batches, final val loss 0.004). v16-q4 vs. v15-q4: complex schema-shift (complex pairs on the 5 unseen domains, 150 of 300) **128** / 110, fixed test **146** / 145; but hold-out 58 / **60**, complex 84 / **89**, v11 / v12 held-out 261 / 216 (**278** / **232**), unresolved-name failures 74 / **42**. v15-q4 was promoted on the Mac instead; see [Evaluation](04-evaluation-and-benchmarking.md#-v16-results-2026-10-09--not-promoted).
- **v14 (2026-10-07) — not promoted:** the v13 retry, v12 continued on `targeted_v14` (10,000 correlated + uncorrelated scalar-subquery pairs side by side + all 708 classic scalar-subquery pairs + large replay = 25,008; 4,800 micro-batches). v14-q4 vs. v12-q4: classic `subquery` family **17 / 15**, 196 / 96 of 200 held-out pairs, 150 / 123 v13 held-out, fixed test **145** / 143 — but names from the training tables leak into unseen schemas (`apppt_date`, `num_stores`, `ss.restock`): hold-out 58 / **59**, v4 hold-out 18 / **21**, v11 / v12 held-out 280 / 227 (287 / 230), complex 82 / 83. The iter-2,400 checkpoint has the same slips (58 / 19, fixed test 139). v12-q4 stays `latest`; see [Evaluation](04-evaluation-and-benchmarking.md#-v14-results-2026-10-07--not-promoted).
- **v9-q4** (`latest` on the Mac 2026-10-01 → 10-02, on the server 10-01 → 10-03): **v9** (trained 2026-10-01): Qwen2.5-Coder-7B continued from v8's adapters (2,400 steps, peak LR 5e-5, final val loss 0.008 / train loss 0.003) on `complex_v9/data_mix` (13,251 pairs) with the **"mirror" window style** (inline Window specs for `OVER (...)` instead of a hoisted block of variables), exported as Q8_0 (`sql2pyspark:v9`) and Q4_K_M (`sql2pyspark:v9-q4`, 4.7 GB). **87/100** on the complex test (v8 60; many-window queries 11–12/15 vs. 1/15); v9 / v9-q4: 59 / 59 hold-out, 134 / **140** fixed test, 18 / **21** v4 hold-out, 0 / 0 real queries. **v9-q4** missed the promotion rule only by one hold-out query; on the server's CPU it generates 4.05 tokens/s vs. v8's 2.47. Rollback: `ollama cp sql2pyspark:v8 sql2pyspark:latest`.
- **v10** (not promoted; superseded by v11 / v12): **v10-q4** / **v10** (trained 2026-10-01 12:18 → 18:42): v9 continued on `classic_v10/data_mix` (12,463 pairs: the classic pairs in the mirror window style + 817 bare `IN` / `NOT IN` pairs), val loss 0.013 → **0.007**. v10-q4 vs. v9-q4: **49 / 11** of 50 held-out `IN` / `NOT IN`, **60** / 59 hold-out, **147** / 140 fixed test, 18 / **21** v4 hold-out, 85 / **87** complex test, 0 / 0 real queries (`real001` no longer loops). Misses the rule on the v4 hold-out and the complex test; see [Evaluation](04-evaluation-and-benchmarking.md#-v10-results-2026-10-01).
- **v11 (`latest` on the Mac 2026-10-02 → 10-03):** **v11-q4** / **v11** (trained 2026-10-02 05:55 → 11:59): v10 continued on `targeted_v11/data_mix` (15,000 pairs: 10,000 new targeted pairs + 5,000 classic_v10 mix), val loss 0.072 → **0.007**. Teaches the rule: *every condition referencing the outer query goes into the join condition (`left_semi`/`left_anti`), only inner-only conditions into the inner `.filter()`*, plus select-before-setop and explode-before-filter (14,700/14,700 raw pairs Spark-verified). v11-q4 vs. v10-q4 / v9-q4: **283** / 83 / 130 of 300 held-out targeted pairs, **21** / 18 / 21 v4 hold-out (`holdout_v4_012`, `017`, `021` fixed), **50** / 49 / 11 `IN` / `NOT IN`, 60 / 60 / 59 hold-out, 143 / 147 / 140 fixed test (classic 93 / 97 / 97: an `OR` became two `.filter()` calls on 2 queries), 84 / 85 / 87 complex test, 0 real queries. Misses the rule on classic (93 vs. 97) and complex (84 vs. 87); see [Evaluation](04-evaluation-and-benchmarking.md#-v11-results-2026-10-02).
- **Previous production model:** **v8** (2026-09-30 → 10-01, kept as `sql2pyspark:v8`): v7 continued for 3,200 steps on `complex_v8/data_mix`: 60/62 hold-out, 139/150 fixed test set, 18/25 v4 hold-out, 60/100 complex test, 0/4 real queries (4/4 and 4/4 with the Kimi K3 repair).
- **Previous models:** v7 (61/62 hold-out, kept as `sql2pyspark:v7`), v6 (the first 7B model, 60/62), v5 (3B, 56/62). Older tags can be rebuilt from their adapters.
- **Agent repair loop:** Kimi (Moonshot AI; Kimi K3.1 Pro in the app since 2026-11-08, earlier results with Kimi K3) integration in `kimi_repair.py`, used by `app.py` and `eval/agent_repair.py` to statically check code (syntax, undefined names, missing `result`, positional `UNION` column ordering via `setop_check`, and ambiguous unqualified columns in aliased joins via `join_check`) and repair it on empty tables with declared schemas.
- **Environment:** Native Apple Silicon training with MLX; Docker setup for Streamlit UI, Spark data pipeline, and evaluation; deployment options for Ubuntu servers with native Ollama, systemd, and Cloudflare Tunnel/Access.
- **Open items (2026-11-12):** (1) the real-world queries stay 0/4 without the Kimi repair; (2) the owner decides whether v17 is promoted (recommended: keep v15-q4). Done: v16's follow-up (v17, not promoted), the move to the Mac mini M6 (2026-11-08, the public site included) and the Qwen3.5-9B runs (stopped).

---

## 📚 Documentation Suite

| Document | Topic | Target Audience | Key Contents |
| :--- | :--- | :--- | :--- |
| [Architecture Overview](01-architecture-overview.md) | High-Level System Architecture | All Developers | End-to-end pipeline diagram, design principles, output conventions (`df_<table>`, `result = ...`, mirror window style, subquery join conditions), Kimi K3 repair loop |
| [Data Engineering Pipeline](02-data-engineering-pipeline.md) | Dataset Synthesis & Verification | Data Engineers, ML Engineers | Table catalog (22 train + 6 hold-out), dual-rendering expression engine (`sqlspec.py`), 12 query families + v4/v5 families (`advanced.py`), `complex_v8` standalone dataset, `complex_v9` mirror window style, `classic_v10` (classic mirror + `IN` / `NOT IN`), `targeted_v11` (correlated subqueries, set operations, LATERAL VIEW filters), Spark dual-execution check |
| [Model Training & Export](03-model-training-and-export.md) | MLX LoRA & GGUF Quantization | ML Engineers, MLOps | Base model progression (3B v1–v5 → 7B v6–v17, Qwen3.5-9B runs), LoRA hyperparameters, crash-proof `train_resumable.py`, training curves, adapter fusion, BF16 dequantization, Q8_0 and Q4_K_M GGUF export, Ollama packaging |
| [Evaluation & Benchmarking](04-evaluation-and-benchmarking.md) | Execution-Based Scoring | ML Engineers, QA | 62 hold-out queries on unseen schemas, 25 v4-feature hold-out queries, 4 real-world queries on their own tables, strict vs. lenient scoring, v1–v17 benchmark results, fixed test set, repetition penalty experiment, Kimi K3 agent repair benchmark |
| [Serving & Streamlit UI](05-serving-and-streamlit-ui.md) | Local Inference & Frontend | Frontend Developers, Users | Ollama `/api/chat` integration, `OLLAMA_URL`, Streamlit UI (`app.py`), code-block extraction, long-SQL repetition window, Kimi K3 static check & empty-table repair, containerized app with Java 17 + PySpark |
| [File Catalog & Reference](06-file-catalog-and-reference.md) | Repository Index | All Contributors | Comprehensive reference of all scripts, datasets, configs, logs, and artifacts in the repository |
| [Operations Runbook](07-operations-runbook.md) | Practical How-To & Makefile Guide | Developers, DevOps | Environment setup, Make targets (`TAG=v9 PREV=v8`), memory limits, Docker profiles, troubleshooting & FAQs |
| [Server Deployment](08-server-deployment.md) | Hosting: Mac mini (since 2026-11-08), formerly Ubuntu + Cloudflare Tunnel | DevOps, Owners | The Mac mini LaunchDaemons (`deploy/macmini/`) and the switch; moving the trained GGUF to an Ubuntu server, Q4_K_M export for CPU inference, RAM/disk sizing, native Ollama + systemd app (`deploy/`), Cloudflare Tunnel & Access for sql2pyspark.praponb.com, updates, troubleshooting |
| [Mac mini Migration](09-mac-mini-migration.md) | Moving to the Mac mini M6 (32 GB) — **done 2026-11-08** | Owners | Plan and record: what to copy (≈185 GB incl. Ollama models and the other `~/GitHub` projects), setup, rebuild, verification checks with pass bars (incl. the Cloudflare tunnel cutover), re-measuring the 16 GB settings, v17 as the first M6 run, wrap-up |
| [User Manual](../USER_MANUAL.md) | End-User Guide & API Reference | End Users, Developers | Web UI walkthrough, Ollama CLI usage, REST API curl/Python examples, SQL feature support, Kimi K3 repair, troubleshooting |
| [Master Project Documentation](../PROJECT_DOCUMENTATION.md) | All-in-One Guide | All Readers | Single-file consolidated summary of the entire documentation suite |

---

## 🚀 Quick Navigation by Persona

### For Data Engineers

- How queries are generated: [Data Engineering Pipeline § 3](02-data-engineering-pipeline.md#3-compositional-query-generator-generate_datasetpy)
- Table schemas and seed data: [Data Engineering Pipeline § 1](02-data-engineering-pipeline.md#1-table-catalog--schema-definitions-schemaspy)
- The Spark verification engine: [Data Engineering Pipeline § 5](02-data-engineering-pipeline.md#5-spark-dual-execution-verifier-verify_pairspy)
- Complex datasets & mirror window style: [Data Engineering Pipeline § 7 & 8](02-data-engineering-pipeline.md#7-the-complex_v8-dataset-v8)

### For Machine Learning Engineers

- Base model selection & LoRA configs: [Model Training & Export § Base Model](03-model-training-and-export.md#🧠-base-model-selection)
- The leakage-free signature split: [Data Engineering Pipeline § 6](02-data-engineering-pipeline.md#6-leakage-free-dataset-splitting-convert_sql_pysparkpy)
- Benchmark metrics and failure analysis: [Evaluation & Benchmarking](04-evaluation-and-benchmarking.md)

### For Developers & Users

- End-user guide & query conversion: [User Manual](../USER_MANUAL.md)
- Start the Streamlit interface: [Operations Runbook § Phase 6](07-operations-runbook.md#phase-6-launch-web-interface)
- Serve the model via Ollama: [Serving & Streamlit UI](05-serving-and-streamlit-ui.md)
- Look up specific files: [File Catalog & Reference](06-file-catalog-and-reference.md)

---

## 🧭 Repository At A Glance

```text
.
├── Makefile                      # Pipeline runner (`make help` lists targets, default TAG=v9 PREV=v8)
├── Modelfile                     # Ollama model definition & system prompt
├── README.md                     # Project summary and results table
├── USER_MANUAL.md                # End-user manual (Web UI, CLI, API, SQL guide)
├── PROJECT_DOCUMENTATION.md      # Consolidated single-file documentation
├── app.py                        # Streamlit web converter UI (Ollama + Kimi K3 repair)
├── kimi_repair.py                # Kimi K3 check + repair loop (app and eval, incl. join_check)
├── requirements.txt              # Pinned dependencies (MLX, PySpark, Streamlit, openai, ...)
├── pyrefly.toml                  # Editor type-checker config (uses the project venv)
├── sql2pyspark.gguf              # Q8_0 GGUF export (8.1 GB); Ollama keeps its own copies
├── sql2pyspark-q4.gguf           # Q4_K_M GGUF export (4.7 GB, compact CPU export)
├── docker-compose.yml            # Docker: app, tools (Spark data/eval) and ollama services
├── docker/                       # Dockerfiles, container requirements, Ollama entrypoint
├── docs/                         # Detailed modular documentation
│   ├── INDEX.md                  # This documentation index
│   ├── 01-architecture-overview.md
│   ├── 02-data-engineering-pipeline.md
│   ├── 03-model-training-and-export.md
│   ├── 04-evaluation-and-benchmarking.md
│   ├── 05-serving-and-streamlit-ui.md
│   ├── 06-file-catalog-and-reference.md
│   ├── 07-operations-runbook.md
│   ├── 08-server-deployment.md
│   └── 09-mac-mini-migration.md
├── deploy/                       # macmini/ (public site since 2026-11-08); former Ubuntu server: sql2pyspark.service, setup_server.sh, update_model.sh
├── sql2pyspark-mlx/              # Data synthesis, verification & MLX training
│   ├── schemas.py                # Table catalog (17 base + 5 extra train, 5 base + 1 extra hold-out)
│   ├── sqlspec.py                # Dual-rendering expression/query objects
│   ├── generate_dataset.py       # Compositional query generator (12 families + v4/v5 families)
│   ├── advanced.py               # v4/v5 stage-based families: pipelines, advanced SQL, nested data
│   ├── complex_v8/               # 10k complex pairs dataset build (20 new tables, 8 families), v8 LoRA config, v8 adapters + train_v8.log
│   ├── complex_v9/               # Mirror window style dataset build (13.2k mix), v9 LoRA config, v9 adapters + train_v9.log
│   ├── classic_v10/              # Classic pairs in mirror style + 900 IN / NOT IN pairs (12.5k mix), v10 LoRA config, v10 adapters + train_v10.log
│   ├── targeted_v11/             # 10k targeted pairs dataset build (correlated subqueries, setops, lateral view), v11 LoRA config, v11 adapters + train_v11.log
│   ├── targeted_v12/             # 4.7k OR / mixed AND-OR pairs + replay (13k mix), v12 LoRA config, v12 adapters + train_v12.log
│   ├── targeted_v13/             # 2k scalar-subquery pairs + replay (11.5k mix), v13 LoRA config, v13 adapters (v13 not used)
│   ├── targeted_v14/             # 10k correlated + uncorrelated scalar-subquery pairs + replay (25k mix), v14 LoRA config, v14 adapters (not promoted)
│   ├── schema_v15/               # synthetic catalog (221 tables) + generators, 10k pairs + schema-shift set (25k mix), v15 LoRA config, v15 adapters
│   ├── complex_v16/              # 10k complex pairs on the synthetic catalog + complex schema-shift set (21k mix), v16 LoRA config, v16 adapters (not promoted)
│   ├── complex_v17/              # v15 mix + 4,000 complex_v16 pairs (29k), v17 LoRA config + adapters (not promoted)
│   ├── qwen35_pilot/             # Qwen3.5-9B pilot + runs 2-9 on the Mac mini: mlx-lm patches, name-copy / complex-mix builders, configs, results (stopped)
│   ├── handwritten_pairs.py      # 105 curated pairs
│   ├── verify_pairs.py           # PySpark dual-execution verifier
│   ├── convert_sql_pyspark.py    # Leakage-free splitter & chat formatter
│   ├── lora_config.yaml          # MLX LoRA training hyperparameters (v7: 7B model, continues from v6)
│   ├── train_resumable.py        # Crash-proof training wrapper used by `make train`
│   ├── train.log                 # Training log (v7; older ones in runs/vN/)
│   ├── data/                     # train.jsonl (16.9k), valid.jsonl, test.jsonl (+ *_meta.jsonl)
│   ├── raw_pairs/                # 20.1k pair files, verified.jsonl & reports
│   ├── adapters/                 # Fine-tuned LoRA weights (v7; v6 in runs/v6/)
│   ├── runs/v2/ … runs/v7/       # Backups: adapters, configs, logs (v2–v5 data only in git history)
│   └── sql2pyspark-model/        # Fused 4-bit MLX model (only between `make fuse` and `make clean`)
├── sql2pyspark-hf/               # Dequantized BF16 fused model (only between `make fuse` and `make clean`)
├── eval/                         # Execution-based evaluation
│   ├── build_holdout.py          # 62 hand-written queries on unseen schemas
│   ├── build_holdout_v4.py       # 25 hand-written queries for the v4 features
│   ├── holdout_v4.jsonl          # v4 feature benchmark
│   ├── test_fixed.jsonl          # Fixed 150-query test set (since v5)
│   ├── holdout.jsonl             # Benchmark query set
│   ├── build_real_queries.py     # 4 real-world production queries checked in Spark
│   ├── real_queries.jsonl        # Real-world query benchmark (real001–real004)
│   ├── real_tables.py            # Sample tables for real-world queries
│   ├── evaluate.py               # Execution-based model evaluation (--tables, --num-predict, --option)
│   ├── agent_repair.py           # Kimi K3 repair loop for failed answers (key in .env)
│   ├── report.py                 # Markdown results-table generator
│   ├── post_training_v4.sh … post_training_v8.sh # Automated post-training scripts
│   ├── start_training_v7.sh … start_training_v17.sh, eval_v14_ckpt.sh, name_slips.py, schema_shift_tables.py, complex_shift_tables.py # Training, eval runners & reports
│   ├── complex_eval_v8.sh        # Complex test benchmarking script
│   ├── complex_v8_test.jsonl     # 300 held-out complex_v8 test pairs (100 sampled, seed 0)
│   ├── logs-repair-rules/        # Console logs for v7 repair with Spark-default rules
│   └── results/                  # <tag>__<set>.json (+ v2..v4-split, v5..v8-repair, v7-repair-rules, v9 / v9-q4 results, v9-q4-repair, v10-q4 results, inlist/, v11-heldout/, v12-heldout/, v13-heldout/, v14-heldout/, schema-shift/, complex-shift/)
└── llama.cpp/                    # Upstream checkout (GGUF converter)
```
