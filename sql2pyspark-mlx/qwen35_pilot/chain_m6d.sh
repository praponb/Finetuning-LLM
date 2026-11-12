#!/bin/zsh
# Qwen3.5-9B run 5 on the Mac mini M6 (2026-11-10): 2,400 iterations on the name-copy data (lora_config_m6d.yaml),
# from run 4 (adapters_m6c), peak lr 3e-5; export to Ollama sql2pyspark:q35m6d-q4, then the key evals vs v17-q4. Ends "ALL DONE".
# Ollama runs as a LaunchDaemon here (com.praponb.ollama), so models are only unloaded, never quit.
# Never touches sql2pyspark:latest / v15-q4 / v17-q4, the root sql2pyspark*.gguf files or the server.
cd /Users/praponb/GitHub/Finetuning-LLM
export PATH=/opt/homebrew/opt/openjdk@17/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin JAVA_HOME=/opt/homebrew/opt/openjdk@17
M=sql2pyspark-mlx; P=$M/qwen35_pilot; PY=$M/.venv/bin/python; BASE=mlx-community/Qwen3.5-9B-4bit; T=q35m6d-q4; R=$P/results
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done }
caffeinate -s -i -w $$ &
mkdir -p $R $P/gguf

if ! grep -q "^exit=0" $P/train_m6d.log 2>/dev/null; then
  unload; step "training (2,400 iterations, name-copy data)"
  (echo "start $(date "+%F %T %z")"
   cd $M && HF_HUB_OFFLINE=1 .venv/bin/python train_resumable.py --config qwen35_pilot/lora_config_m6d.yaml --launcher qwen35_pilot/lora_q35.py --max-restarts 20
   echo "exit=$?"; echo "end $(date "+%F %T %z")") >> $P/train_m6d.log 2>&1
fi
grep -q "^exit=0" $P/train_m6d.log || { step "training failed; stopping"; exit 1; }

if [ ! -s $P/gguf/m6d-q4.gguf ]; then
  step "export: fuse (dequantized) -> fix_norms -> f16 GGUF -> Q4_K_M"
  rm -rf $P/hf_m6d
  (cd $M && HF_HUB_OFFLINE=1 .venv/bin/mlx_lm.fuse --model $BASE --adapter-path qwen35_pilot/adapters_m6d --save-path qwen35_pilot/hf_m6d --dequantize) || { step "fuse failed"; exit 1; }
  $PY $P/fix_norms.py $P/hf_m6d || { step "fix_norms failed"; exit 1; }
  $PY llama.cpp/convert_hf_to_gguf.py $P/hf_m6d --outtype f16 --no-mtp --outfile $P/gguf/m6d-f16.gguf || { step "gguf failed"; exit 1; }
  llama.cpp/build/bin/llama-quantize $P/gguf/m6d-f16.gguf $P/gguf/m6d-q4.gguf Q4_K_M > $P/quantize_m6d.log 2>&1 || { step "quantize failed"; exit 1; }
  rm -rf $P/hf_m6d $P/gguf/m6d-f16.gguf; ls -la $P/gguf
fi
sed 's#^FROM .*#FROM ./gguf/m6d-q4.gguf#' $P/Modelfile.q35 > $P/Modelfile.q35m6d
(cd $P && ollama create sql2pyspark:$T -f Modelfile.q35m6d) || { step "ollama create failed"; exit 1; }

E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
step "$T holdout (62)";       E --set eval/holdout.jsonl    --out $R/${T}__holdout.json
step "$T holdout_v4 (25)";    E --set eval/holdout_v4.jsonl --out $R/${T}__holdout_v4.json
step "$T test_fixed (150)";   E --set eval/test_fixed.jsonl --out $R/${T}__test_fixed.json
step "$T complex (100)";      E --set eval/complex_v8_test.jsonl --tables $M/complex_v8/eval_tables.py --sample 100 --seed 0 --out $R/${T}__complex_v8_test.json
step "$T complex-shift (150)"; E --set $M/complex_v16/data/complex_shift_meta.jsonl --sample 150 --seed 0 --tables eval/complex_shift_tables.py --out $R/${T}__complex_shift.json
unload
step "ALL DONE"
