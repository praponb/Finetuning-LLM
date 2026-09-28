#!/bin/zsh
# v7 post-training chain: wait for training, export, evaluate v7 on the 4 eval sets, restart Docker + the app, then
# run the Kimi K3 repair on v7's real-query answers (sample and empty tables). Log: eval/post_training_v7.log.
cd /Users/praponb/GitHub/Finetuning-LLM
PY=sql2pyspark-mlx/.venv/bin/python
step() { echo "[$(date +%H:%M:%S)] $*"; }
until grep -q "^exit=" sql2pyspark-mlx/train.log; do sleep 30; done
grep -q "^exit=0" sql2pyspark-mlx/train.log || { step "training failed; stopping"; exit 1; }
step "export v7"; make export TAG=v7 || { step "export failed"; exit 1; }
step "v7 holdout";      $PY eval/evaluate.py --model sql2pyspark:v7 --set eval/holdout.jsonl --timeout 600
step "v7 holdout_v4";   $PY eval/evaluate.py --model sql2pyspark:v7 --set eval/holdout_v4.jsonl --timeout 600
step "v7 real_queries"; $PY eval/evaluate.py --model sql2pyspark:v7 --set eval/real_queries.jsonl --tables eval/real_tables.py --timeout 600
step "v7 test_fixed";   $PY eval/evaluate.py --model sql2pyspark:v7 --set eval/test_fixed.jsonl --timeout 600
step "restart Docker + app"; open -a "Docker Desktop"; until docker info >/dev/null 2>&1; do sleep 5; done
docker compose up -d app
step "v7 Kimi repair (sample tables)"; $PY eval/agent_repair.py --results eval/results/v7__real_queries.json > eval/agent_repair_v7.log 2>&1
step "v7 Kimi repair (empty tables)";  $PY eval/agent_repair.py --results eval/results/v7__real_queries.json --mode empty > eval/agent_repair_v7_empty.log 2>&1
step "ALL DONE"
