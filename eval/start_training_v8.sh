#!/bin/zsh
# v8 launcher: check the complex_v8 data, stop Docker and Ollama models (16 GB M2), train from the v7 adapters with
# complex_v8/lora_config.yaml (data_mix: 10,000 new complex pairs + 7,000 v7 pairs), then run the post-training
# chain. Logs: eval/start_training_v8.log, sql2pyspark-mlx/complex_v8/train_v8.log, eval/post_training_v8.log
cd /Users/praponb/GitHub/Finetuning-LLM
step() { echo "[$(date +%H:%M:%S)] $*"; }
D=sql2pyspark-mlx/complex_v8/data_mix
[ -s $D/train.jsonl ] && [ -s $D/valid.jsonl ] || { step "complex_v8/data_mix missing; stopping"; exit 1; }
[ -s sql2pyspark-mlx/runs/v7/adapters/adapters.safetensors ] || { step "v7 backup missing; stopping"; exit 1; }
step "data ok: $(wc -l < $D/train.jsonl) train / $(wc -l < $D/valid.jsonl) valid"
if docker info >/dev/null 2>&1; then
  step "stopping Docker"; docker compose down; osascript -e 'quit app "Docker Desktop"'
  until ! pgrep -f "com.docker.virtualization|Docker Desktop.app/Contents/MacOS" >/dev/null; do sleep 2; done
fi
for m in $(ollama ps | awk 'NR>1{print $1}'); do ollama stop $m; done
step "start training"
(echo "start $(date "+%Y-%m-%d %H:%M:%S %z")"
 cd sql2pyspark-mlx && .venv/bin/python train_resumable.py --config complex_v8/lora_config.yaml
 echo "exit=$?") > sql2pyspark-mlx/complex_v8/train_v8.log 2>&1 &
sleep 5
zsh eval/post_training_v8.sh > eval/post_training_v8.log 2>&1
step "chain finished"
