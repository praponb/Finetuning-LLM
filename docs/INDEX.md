# SQL → PySpark Fine-Tuning: Documentation Index

Welcome to the technical documentation for the **SQL → PySpark Fine-Tuning Pipeline**. This project implements an end-to-end, local machine learning and data engineering workflow on Apple Silicon to fine-tune **Qwen2.5-Coder-3B-Instruct** for translating Spark SQL queries into idiomatic PySpark DataFrame code.

**Status (2026-09-28):** `sql2pyspark:latest` is **v5** (continued from v4 on a replay-heavy dataset with generator fixes): 128/150 on the fixed test set (v3 105, v4 110), 95/97 on its classic queries, and 14/25 on the v4-feature hold-out (v4 9, v3 3), but 56/62 on the original hold-out (v3 59) and 0/4 on the real-world queries. It missed two promotion-rule conditions and was promoted by the owner's decision; v3 is kept as `sql2pyspark:v3` for rollback. The Docker setup works (app, Spark data pipeline, evaluation). The code is in the private GitHub repo `praponb/Finetuning-LLM`.

---

## 📚 Documentation Suite

| Document | Topic | Target Audience | Key Contents |
| :--- | :--- | :--- | :--- |
| [Architecture Overview](01-architecture-overview.md) | High-Level System Architecture | All Developers | End-to-end pipeline diagram, design principles, output conventions (`df_<table>`, `result = ...`) |
| [Data Engineering Pipeline](02-data-engineering-pipeline.md) | Dataset Synthesis & Verification | Data Engineers, ML Engineers | Table catalog (17 base + 5 extra train / 5 base + 1 extra hold-out), dual-rendering expression engine (`sqlspec.py`), 12 query families + the v4 stage-based families (`advanced.py`), v3 generator changes, Spark dual-execution check, leakage-free split |
| [Model Training & Export](03-model-training-and-export.md) | MLX LoRA & GGUF Quantization | ML Engineers, MLOps | LoRA hyperparameters (v4: batch 2 × 2, checkpointing, 3,200 tokens), crash-proof `train_resumable.py`, training curve, adapter fusion, BF16 dequantization, llama.cpp Q8_0 export, Ollama packaging |
| [Evaluation & Benchmarking](04-evaluation-and-benchmarking.md) | Execution-Based Scoring | ML Engineers, QA | 62 hold-out queries on unseen schemas, 25 v4-feature hold-out queries, 4 real-world queries on their own tables, strict vs. lenient scoring, v1–v5 results, fixed test set, promotion rule and the v4/v5 decisions, failure analysis and v4 targets |
| [Serving & Streamlit UI](05-serving-and-streamlit-ui.md) | Local Inference & Frontend | Frontend Developers, Users | Ollama `/api/chat` integration, `OLLAMA_URL`, Streamlit UI (`app.py`), code-block extraction, model tag field, app in Docker |
| [File Catalog & Reference](06-file-catalog-and-reference.md) | Repository Index | All Contributors | Reference of the scripts, datasets, configs and artifacts in the repository |
| [Operations Runbook](07-operations-runbook.md) | Practical How-To & Makefile Guide | Developers, DevOps | Environment setup, Make targets, memory limits, Docker, troubleshooting |
| [Master Project Documentation](../PROJECT_DOCUMENTATION.md) | All-in-One Guide | All Readers | Single-file summary of everything above |

---

## 🚀 Quick Navigation by Persona

### For Data Engineers
- How queries are generated: [Data Engineering Pipeline § 3](02-data-engineering-pipeline.md#3-compositional-query-generator-generate_datasetpy)
- Table schemas and seed data: [Data Engineering Pipeline § 1](02-data-engineering-pipeline.md#1-table-catalog--schema-definitions-schemaspy)
- The Spark verification engine: [Data Engineering Pipeline § 5](02-data-engineering-pipeline.md#5-spark-dual-execution-verifier-verify_pairspy)

### For Machine Learning Engineers
- LoRA training config & parameters: [Model Training & Export](03-model-training-and-export.md)
- The leakage-free signature split: [Data Engineering Pipeline § 6](02-data-engineering-pipeline.md#6-leakage-free-dataset-splitting-convert_sql_pysparkpy)
- Benchmark metrics and failure analysis: [Evaluation & Benchmarking](04-evaluation-and-benchmarking.md)

### For Developers & Users
- Start the Streamlit interface: [Operations Runbook § Phase 6](07-operations-runbook.md#phase-6-launch-web-interface)
- Serve the model via Ollama: [Serving & Streamlit UI](05-serving-and-streamlit-ui.md)
- Look up specific files: [File Catalog & Reference](06-file-catalog-and-reference.md)

---

## 🧭 Repository At A Glance

```
.
├── Makefile                      # Pipeline runner (`make help` lists targets)
├── Modelfile                     # Ollama model definition & system prompt
├── README.md                     # Project summary and results table
├── PROJECT_DOCUMENTATION.md      # Consolidated single-file documentation
├── app.py                        # Streamlit web converter UI
├── kimi_repair.py                # Kimi K3 check + repair (app and eval)
├── requirements.txt              # Pinned dependencies (MLX, PySpark, Streamlit, ...)
├── pyrefly.toml                  # Editor type-checker config (uses the project venv)
├── sql2pyspark.gguf              # Q8_0 GGUF of the last export (v5, 3.3 GB); Ollama keeps its own copies
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
│   └── 07-operations-runbook.md
├── sql2pyspark-mlx/              # Data synthesis, verification & MLX training
│   ├── schemas.py                # Table catalog (17 base + 5 extra train, 5 base + 1 extra hold-out)
│   ├── sqlspec.py                # Dual-rendering expression/query objects
│   ├── generate_dataset.py       # Compositional query generator (12 families + v4 families)
│   ├── advanced.py               # v4 stage-based families: pipelines, advanced SQL, nested data
│   ├── handwritten_pairs.py      # 105 curated pairs
│   ├── verify_pairs.py           # PySpark dual-execution verifier
│   ├── convert_sql_pyspark.py    # Leakage-free splitter & chat formatter
│   ├── lora_config.yaml          # MLX LoRA training hyperparameters
│   ├── train_resumable.py        # Crash-proof training wrapper used by `make train`
│   ├── train.log                 # Training log of the v5 run
│   ├── data/                     # train.jsonl (16.9k), valid.jsonl, test.jsonl (+ *_meta.jsonl)
│   ├── raw_pairs/                # 15.1k pair files, verified.jsonl & reports
│   ├── adapters/                 # Fine-tuned LoRA weights + checkpoints (v5)
│   ├── runs/v2/ … runs/v4/       # v2–v4 backups: adapters, data, raw pairs, logs
│   └── sql2pyspark-model/        # Fused 4-bit MLX model
├── sql2pyspark-hf/               # Dequantized BF16 fused model (input for GGUF)
├── eval/                         # Execution-based evaluation
│   ├── build_holdout.py          # 62 hand-written queries on unseen schemas
│   ├── build_holdout_v4.py       # 25 hand-written queries for the v4 features
│   ├── holdout_v4.jsonl          # v4 feature benchmark
│   ├── test_fixed.jsonl          # fixed 150-query test set (since v5)
│   ├── holdout.jsonl             # Benchmark query set
│   ├── build_real_queries.py     # 4 real-world production queries checked in Spark
│   ├── real_queries.jsonl        # Real-world query benchmark (real001–real004)
│   ├── real_tables.py            # Sample tables for real-world queries
│   ├── evaluate.py               # Execution-based model evaluation (--tables, --num-predict)
│   ├── agent_repair.py           # Kimi K3 repair loop for failed answers (key in .env)
│   ├── report.py                 # Markdown results-table generator
│   └── results/                  # <tag>__<set>.json results (+ v2-split/, v3-split/, v4-split/)
└── llama.cpp/                    # Upstream checkout (GGUF converter)
```
