#!/bin/zsh
# v7 launcher (unattended): wait for the v7 data build and the v6 Kimi repair runs, check the data, stop Docker and
# Ollama models, then start training and the post-training chain. Log: eval/start_training_v7.log
cd /Users/praponb/GitHub/Finetuning-LLM
step() { echo "[$(date +%H:%M:%S)] $*"; }
until grep -q "^exit=" sql2pyspark-mlx/data_v7.log; do sleep 30; done
grep -q "^exit=0" sql2pyspark-mlx/data_v7.log || { step "data build failed; stopping"; exit 1; }
[ -s sql2pyspark-mlx/data/train.jsonl ] || { step "no train.jsonl; stopping"; exit 1; }
step "data ok: $(wc -l < sql2pyspark-mlx/data/train.jsonl) train samples"
while pgrep -f "agent_repair.py" >/dev/null; do sleep 30; done
step "repair runs finished; stopping Docker and Ollama models"
docker compose down; osascript -e 'quit app "Docker Desktop"'
until ! pgrep -f "com.docker.virtualization|Docker Desktop.app/Contents/MacOS" >/dev/null; do sleep 2; done
for m in $(ollama ps | awk 'NR>1{print $1}'); do ollama stop $m; done
step "start training"
(echo "start $(date "+%Y-%m-%d %H:%M:%S %z")"; make train; echo "exit=$?") > sql2pyspark-mlx/train.log 2>&1 &
sleep 5
zsh eval/post_training_v7.sh > eval/post_training_v7.log 2>&1
step "chain finished"
