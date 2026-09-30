# SQL → PySpark Fine-Tuning: Documentation Index

Welcome to the technical documentation for the **SQL → PySpark Fine-Tuning Pipeline**. This project implements an end-to-end, local machine learning and data engineering workflow on Apple Silicon to fine-tune **Qwen2.5-Coder** (v1–v5: 3B; v6: 7B) for translating Spark SQL queries into idiomatic PySpark DataFrame code.

**Status (2026-09-29):**
- **Previous production model:** **v7** (latest from 2026-09-29 to 2026-09-30, kept as `sql2pyspark:v7`): Qwen2.5-Coder-7B continued from v6's adapters (2,400 steps, peak LR 5e-5, 4 h 58 min) on the v7 dataset (DATEDIFF arithmetic, unrelated-column UNIONs, windows partitioned by date expressions): 61/62 hold-out, 137/150 fixed test set (97/97 classic), 16/25 v4 hold-out, 0/4 real queries (3/4 or 4/4 with the Kimi K3 repair). v6 (the first 7B model, 60/62 and 132/150) is kept as `sql2pyspark:v6` for rollback, v5 (3B) as `sql2pyspark:v5`. About 2x slower than the 3B, ~8 GB in Ollama.
- **Production model:** `sql2pyspark:latest` is **v8** (since 2026-09-30, promoted by the owner): v7 continued for 3,200 steps (11 h 42 min) on `complex_v8/data_mix` (10,000 new verified complex pairs on 20 new tables + 7,000 v7 pairs): 60/62 hold-out (one miss more than v7), 139/150 fixed test set (97/97 classic), **18/25** v4 hold-out, 0/4 real queries (**4/4 and 4/4** with the Kimi K3 repair), **60/100** on the new complex test (v7 31). Rollback: `ollama cp sql2pyspark:v7 sql2pyspark:latest`.
- **Agent repair loop:** Kimi K3 (Moonshot AI) integration in `kimi_repair.py`, used by `app.py` and `eval/agent_repair.py` to statically check code (syntax, undefined names, `result`, and positional `UNION` column ordering) and repair it on empty tables with declared schemas (v7's real queries with the Spark-default rules: 11/12 repairs on sample tables and 12/12 on empty tables over 3 runs per mode). Default UI mode runs when a check fails or the SQL is long (> 2,000 characters).
- **Environment:** Native Apple Silicon training with MLX; Docker setup for Streamlit UI, Spark data pipeline, and evaluation. The codebase is in the private GitHub repo `praponb/Finetuning-LLM`.

---

## 📚 Documentation Suite

| Document | Topic | Target Audience | Key Contents |
| :--- | :--- | :--- | :--- |
| [Architecture Overview](01-architecture-overview.md) | High-Level System Architecture | All Developers | End-to-end pipeline diagram, design principles, output conventions (`df_<table>`, `result = ...`), Kimi K3 repair loop |
| [Data Engineering Pipeline](02-data-engineering-pipeline.md) | Dataset Synthesis & Verification | Data Engineers, ML Engineers | Table catalog (17 base + 5 extra train / 5 base + 1 extra hold-out), dual-rendering expression engine (`sqlspec.py`), 12 query families + the v4/v5 stage-based families (`advanced.py`), Spark dual-execution check, leakage-free signature split |
| [Model Training & Export](03-model-training-and-export.md) | MLX LoRA & GGUF Quantization | ML Engineers, MLOps | Base model progression (3B v1–v5 → 7B v6), LoRA hyperparameters (v6: batch 1 × 4, 7B memory profiling; v4/v5: batch 2 × 2), crash-proof `train_resumable.py`, training curves, adapter fusion, BF16 dequantization, llama.cpp Q8_0 export, Ollama packaging |
| [Evaluation & Benchmarking](04-evaluation-and-benchmarking.md) | Execution-Based Scoring | ML Engineers, QA | 62 hold-out queries on unseen schemas, 25 v4-feature hold-out queries, 4 real-world queries on their own tables, strict vs. lenient scoring, v1–v5 benchmark results, fixed test set, repetition penalty experiment, Kimi K3 agent repair benchmark |
| [Serving & Streamlit UI](05-serving-and-streamlit-ui.md) | Local Inference & Frontend | Frontend Developers, Users | Ollama `/api/chat` integration, `OLLAMA_URL`, Streamlit UI (`app.py`), code-block extraction, long-SQL repetition window, Kimi K3 static check & empty-table repair, containerized app with Java 17 + PySpark |
| [File Catalog & Reference](06-file-catalog-and-reference.md) | Repository Index | All Contributors | Comprehensive reference of all scripts, datasets, configs, logs, and artifacts in the repository |
| [Operations Runbook](07-operations-runbook.md) | Practical How-To & Makefile Guide | Developers, DevOps | Environment setup, Make targets (`TAG=v9 PREV=v8`), memory limits, Docker profiles, troubleshooting & FAQs |
| [Server Deployment](08-server-deployment.md) | Hosting on Ubuntu + Cloudflare Tunnel | DevOps, Owners | Moving the trained GGUF to an Ubuntu server, RAM/disk sizing, native Ollama + systemd app (`deploy/`), Cloudflare Tunnel & Access for sql2pyspark.praponb.com, updates, troubleshooting (502, 1033, missing module) |
| [User Manual](../USER_MANUAL.md) | End-User Guide & API Reference | End Users, Developers | Web UI walkthrough, Ollama CLI usage, REST API curl/Python examples, SQL feature support, Kimi K3 repair, troubleshooting |
| [Master Project Documentation](../PROJECT_DOCUMENTATION.md) | All-in-One Guide | All Readers | Single-file consolidated summary of the entire documentation suite |

---

## 🚀 Quick Navigation by Persona

### For Data Engineers
- How queries are generated: [Data Engineering Pipeline § 3](02-data-engineering-pipeline.md#3-compositional-query-generator-generate_datasetpy)
- Table schemas and seed data: [Data Engineering Pipeline § 1](02-data-engineering-pipeline.md#1-table-catalog--schema-definitions-schemaspy)
- The Spark verification engine: [Data Engineering Pipeline § 5](02-data-engineering-pipeline.md#5-spark-dual-execution-verifier-verify_pairspy)

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

```
.
├── Makefile                      # Pipeline runner (`make help` lists targets, default TAG=v9 PREV=v8)
├── Modelfile                     # Ollama model definition & system prompt
├── README.md                     # Project summary and results table
├── USER_MANUAL.md                # End-user manual (Web UI, CLI, API, SQL guide)
├── PROJECT_DOCUMENTATION.md      # Consolidated single-file documentation
├── app.py                        # Streamlit web converter UI (Ollama + Kimi K3 repair)
├── kimi_repair.py                # Kimi K3 check + repair loop (app and eval)
├── requirements.txt              # Pinned dependencies (MLX, PySpark, Streamlit, openai, ...)
├── pyrefly.toml                  # Editor type-checker config (uses the project venv)
├── sql2pyspark.gguf              # Q8_0 GGUF of the last export (v8, 8.1 GB); Ollama keeps its own copies
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
│   └── 08-server-deployment.md
├── deploy/                       # Ubuntu server: sql2pyspark.service (systemd), setup_server.sh, update_model.sh
├── sql2pyspark-mlx/              # Data synthesis, verification & MLX training
│   ├── schemas.py                # Table catalog (17 base + 5 extra train, 5 base + 1 extra hold-out)
│   ├── sqlspec.py                # Dual-rendering expression/query objects
│   ├── generate_dataset.py       # Compositional query generator (12 families + v4/v5 families)
│   ├── advanced.py               # v4/v5 stage-based families: pipelines, advanced SQL, nested data
│   ├── complex_v8/               # 10k complex pairs dataset build (20 new tables, 8 families), v8 LoRA config, v8 adapters + train_v8.log
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
│   ├── post_training_v4.sh       # v4 post-training script
│   ├── post_training_v5.sh       # v5 post-training script
│   ├── post_training_v6.sh       # v6 post-training script (timeout 600s for 7B)
│   ├── start_training_v7.sh      # v7 unattended training launcher
│   ├── post_training_v7.sh       # v7 post-training export + eval chain
│   ├── start_training_v8.sh      # v8 launcher (complex_v8 data mix)
│   ├── post_training_v8.sh       # v8 export + eval + Kimi repair chain
│   ├── complex_eval_v8.sh        # v7 vs v8 on the complex_v8 test sample
│   ├── complex_v8_test.jsonl     # 300 held-out complex_v8 test pairs (100 sampled, seed 0)
│   ├── logs-repair-rules/        # Console logs for v7 repair with Spark-default rules
│   └── results/                  # <tag>__<set>.json (+ v2..v4-split, v5..v8-repair, v7-repair-rules)
└── llama.cpp/                    # Upstream checkout (GGUF converter)
```
