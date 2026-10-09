#!/bin/zsh
# Qwen3.5-9B pilot, steps 3-6 + the untuned-base comparison (README.md has the plan and the results).
#   3. short training run (lora_config.yaml; train_resumable.py --launcher lora_q35.py = mlx_lm.lora + gd_patch + chunked_loss)
#   4. export: fuse --dequantize -> fix_norms.py -> f16 GGUF (--no-mtp) -> Q4_K_M -> ollama create sql2pyspark:q35-pilot-q4
#   5. hold-out (62) for the pilot (sign of life only; the 5-query smoke test is the first 5 replies)
#   6. CPU-only speed vs v15-q4
#   +  hold-out for the untuned bases: stock Qwen3.5-9B Q4_K_M and Qwen2.5-Coder-7B-Instruct Q4_K_M, same system prompt
# Qwen3.5 models are queried with Ollama's think=false: Ollama renders qwen35 with its built-in Qwen3.5 template and
# parser (the Modelfile TEMPLATE is not used), and without the flag the whole reply lands in message.thinking.
# Never touches sql2pyspark:latest / v15-q4, the root sql2pyspark*.gguf files or the server.
# Log: qwen35_pilot/chain.log (this script), qwen35_pilot/train.log (training).
cd /Users/praponb/GitHub/Finetuning-LLM
M=sql2pyspark-mlx
P=$M/qwen35_pilot
PY=$M/.venv/bin/python
BASE=mlx-community/Qwen3.5-9B-4bit
step() { echo "[$(date +%H:%M:%S)] $*"; }
caffeinate -s -i -w $$ &
mkdir -p $P/results

if ! grep -q "^exit=0" $P/train.log 2>/dev/null; then
  pgrep -x ollama >/dev/null && { step "Ollama is running; stopping (training needs the memory)"; osascript -e 'quit app "Ollama"'; sleep 5; }
  step "step 3: training"
  (echo "start $(date "+%F %T %z")"
   cd $M && HF_HUB_OFFLINE=1 .venv/bin/python train_resumable.py --config qwen35_pilot/lora_config.yaml --launcher qwen35_pilot/lora_q35.py --max-restarts 20
   echo "exit=$?"; echo "end $(date "+%F %T %z")") >> $P/train.log 2>&1
fi
grep -q "^exit=0" $P/train.log || { step "training failed; stopping"; exit 1; }

if [ ! -s $P/gguf/pilot-q4.gguf ]; then
  step "step 4: fuse (dequantized)"
  rm -rf $P/hf
  (cd $M && HF_HUB_OFFLINE=1 .venv/bin/mlx_lm.fuse --model $BASE --adapter-path qwen35_pilot/adapters --save-path qwen35_pilot/hf --dequantize) \
    || { step "fuse failed"; exit 1; }
  du -sh $P/hf
  step "step 4: undo the mlx norm shift"; $PY $P/fix_norms.py $P/hf || { step "fix_norms failed"; exit 1; }
  step "step 4: f16 GGUF"
  $PY llama.cpp/convert_hf_to_gguf.py $P/hf --outtype f16 --no-mtp --outfile $P/gguf/pilot-f16.gguf || { step "gguf failed"; exit 1; }
  step "step 4: Q4_K_M"
  llama.cpp/build/bin/llama-quantize $P/gguf/pilot-f16.gguf $P/gguf/pilot-q4.gguf Q4_K_M > $P/quantize.log 2>&1 || { step "quantize failed"; exit 1; }
  ls -la $P/gguf
fi

if ! curl -s localhost:11434/api/version >/dev/null; then
  step "start Ollama"; open -a Ollama
  until curl -s localhost:11434/api/version >/dev/null; do sleep 2; done
fi
ollama show sql2pyspark:q35-pilot-q4 >/dev/null 2>&1 || ollama create sql2pyspark:q35-pilot-q4 -f $P/Modelfile.q35 || { step "ollama create failed"; exit 1; }

step "step 5: q35-pilot-q4 hold-out (62)"
$PY eval/evaluate.py --model sql2pyspark:q35-pilot-q4 --set eval/holdout.jsonl --timeout 600 --think false --out $P/results/q35-pilot-q4__holdout.json

step "step 6: CPU-only speed (num_gpu 0) v15-q4 vs q35-pilot-q4"
$PY $P/speed_test.py sql2pyspark:v15-q4 sql2pyspark:q35-pilot-q4 | tee $P/results/speed.jsonl

step "base comparison: create untuned models"
sed 's#^FROM .*#FROM ./gguf/Qwen3.5-9B-Q4_K_M.gguf#' $P/Modelfile.q35 > $P/Modelfile.base-q35
ollama create sql2pyspark:base-q35-q4 -f $P/Modelfile.base-q35 || step "create base-q35-q4 failed"
sed 's#^FROM .*#FROM ./gguf/qwen2.5-coder-7b-instruct-q4_k_m.gguf#' Modelfile > $P/Modelfile.base-coder7b
ollama create sql2pyspark:base-coder7b-q4 -f $P/Modelfile.base-coder7b || step "create base-coder7b-q4 failed"
step "base-q35-q4 hold-out (62)"
$PY eval/evaluate.py --model sql2pyspark:base-q35-q4 --set eval/holdout.jsonl --timeout 600 --think false --out $P/results/base-q35-q4__holdout.json
step "base-coder7b-q4 hold-out (62)"
$PY eval/evaluate.py --model sql2pyspark:base-coder7b-q4 --set eval/holdout.jsonl --timeout 600 --out $P/results/base-coder7b-q4__holdout.json
for m in $(ollama ps | awk 'NR>1{print $1}'); do ollama stop $m; done
step "ALL DONE"
