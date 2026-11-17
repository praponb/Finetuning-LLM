#!/bin/zsh
# Qwen3.5-35B-A3B feasibility, step 2: export the UNTUNED 4-bit base the same way a trained model will be exported
# (dequantize -> fix_norms -> f16 GGUF without MTP -> Q4_K_M -> Ollama) and score it zero-shot on hold-out + v4.
# Proves the export/serving path (llama.cpp qwen35moe, Ollama 0.40 support, no think leak) before any training.
# Log: sql2pyspark-mlx/qwen35moe/step2.log (this script's output). Run from the repo root.
cd /Users/praponb/GitHub/Finetuning-LLM
M=sql2pyspark-mlx; D=$M/qwen35moe; PY=$M/.venv/bin/python; BASE=mlx-community/Qwen3.5-35B-A3B-4bit; T=q35moe-base-q4
R=$D/results; mkdir -p $R $D/gguf
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done }
caffeinate -s -i -w $$ &

if [ ! -s $D/gguf/base-q4.gguf ]; then
  unload
  if [ ! -f $D/hf_base/.fixed ]; then
  step "dequantize the 4-bit base (mlx_lm.convert -d) -> $D/hf_base"
  rm -rf $D/hf_base
  HF_HUB_OFFLINE=1 $M/.venv/bin/mlx_lm.convert --hf-path $BASE --mlx-path $D/hf_base --dequantize || { step "dequantize failed"; exit 1; }
  du -sh $D/hf_base
  step "fix_norms (undo mlx-lm's +1 RMSNorm shift)"
  $PY $M/qwen35_pilot/fix_norms.py $D/hf_base || { step "fix_norms failed"; exit 1; }
  step "fix_experts (switch_mlp -> HF experts.gate_up_proj / down_proj)"
  $PY $D/fix_experts.py $D/hf_base || { step "fix_experts failed"; exit 1; }
  touch $D/hf_base/.fixed
  fi
  step "convert to f16 GGUF (--no-mtp)"
  $PY llama.cpp/convert_hf_to_gguf.py $D/hf_base --outtype f16 --no-mtp --outfile $D/gguf/base-f16.gguf || { step "gguf failed"; exit 1; }
  step "quantize Q4_K_M"
  llama.cpp/build/bin/llama-quantize $D/gguf/base-f16.gguf $D/gguf/base-q4.gguf Q4_K_M > $D/quantize_base.log 2>&1 || { step "quantize failed"; exit 1; }
  rm -rf $D/hf_base $D/gguf/base-f16.gguf; ls -la $D/gguf
fi

sed 's#^FROM .*#FROM ./gguf/base-q4.gguf#' $M/qwen35_pilot/Modelfile.q35m6g > $D/Modelfile.base
(cd $D && ollama create sql2pyspark:$T -f Modelfile.base) || { step "ollama create failed"; exit 1; }

step "smoke: one conversion"
curl -s 127.0.0.1:11434/api/generate -d "{\"model\":\"sql2pyspark:$T\",\"think\":false,\"prompt\":\"Convert SQL to PySpark:\\n\`\`\`sql\\nSELECT region, COUNT(DISTINCT customer_id) AS customers FROM orders WHERE total > 100 GROUP BY region;\\n\`\`\`\",\"stream\":false,\"options\":{\"temperature\":0}}" \
  | $PY -c "import sys,json; d=json.load(sys.stdin); r=d.get('response',''); print(r[:800]); print('think leak:', '<think>' in r or '</think>' in r, '| thinking field:', bool(d.get('thinking'))); print('eval tok/s:', round(d['eval_count']/(d['eval_duration']/1e9),1), '| load s:', round(d['load_duration']/1e9,1))" \
  || { step "smoke failed"; exit 1; }
ollama ps

E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
step "$T holdout (62, zero-shot)";    E --set eval/holdout.jsonl    --out $R/${T}__holdout.json
step "$T holdout_v4 (25, zero-shot)"; E --set eval/holdout_v4.jsonl --out $R/${T}__holdout_v4.json
unload
step "STEP2 DONE"
