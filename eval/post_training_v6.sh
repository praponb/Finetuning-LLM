#!/bin/zsh
# v6 (7B) post-training chain: wait for training, export, evaluate v6 on the 4 eval sets, restart Docker + the app.
# Log: eval/post_training_v6.log. (v5 results on the same sets already exist.) The 7B generates ~2x slower than the
# 3B, so the per-query timeout is raised to 600 s.
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
step() { echo "[$(date +%H:%M:%S)] $*"; }
until grep -q "^exit=" sql2pyspark-mlx/train.log; do sleep 30; done
grep -q "^exit=0" sql2pyspark-mlx/train.log || { step "training failed; stopping"; exit 1; }
step "export v6"; make export TAG=v6 || { step "export failed"; exit 1; }
step "v6 holdout";      $PY eval/evaluate.py --model sql2pyspark:v6 --set eval/holdout.jsonl --timeout 600
step "v6 holdout_v4";   $PY eval/evaluate.py --model sql2pyspark:v6 --set eval/holdout_v4.jsonl --timeout 600
step "v6 real_queries"; $PY eval/evaluate.py --model sql2pyspark:v6 --set eval/real_queries.jsonl --tables eval/real_tables.py --timeout 600
step "v6 test_fixed";   $PY eval/evaluate.py --model sql2pyspark:v6 --set eval/test_fixed.jsonl --timeout 600
step "restart Docker + app"; open -a "Docker Desktop"; until docker info >/dev/null 2>&1; do sleep 5; done
docker compose up -d app
step "ALL DONE"
