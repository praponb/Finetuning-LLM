#!/bin/zsh
# Qwen3.5-35B-A3B feasibility, steps 3-5 (runs after the Kimi pilot has stopped; the site's model is unloaded):
#   3. memory test on the 64 longest samples: try (seq 3200, 16 layers, full keys) -> (2600, 16) -> (2600, 8) ->
#      (2600, 8, attention-only keys); the first config that finishes 30 micro-batches without an abort wins
#   4. speed test: 200 micro-batches on the v18a mix (filtered to the chosen seq) with fresh adapters, lr 1e-4
#   5. export the 200-step adapter (fuse --dequantize -> fix_norms -> fix_experts -> f16 GGUF -> Q4_K_M -> Ollama
#      sql2pyspark:q35moe-pilot-q4) and score hold-out + v4 (compare with the zero-shot q35moe-base-q4: 1/62, 0/25)
# Log: qwen35moe/step345.log (this script). Run from the repo root.
cd /Users/praponb/GitHub/Finetuning-LLM
M=sql2pyspark-mlx; D=$M/qwen35moe; PY=$M/.venv/bin/python; BASE=mlx-community/Qwen3.5-35B-A3B-4bit; T=q35moe-pilot-q4
R=$D/results; mkdir -p $R $D/gguf
export HF_HUB_OFFLINE=1
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done }
caffeinate -s -i -w $$ &

step "waiting for the Kimi pilot to stop"
while pgrep -f "realstyle_v19/generate.py" >/dev/null; do sleep 60; done
step "pilot stopped"; unload

# ---- 3. memory test -------------------------------------------------------------------------------------------
ATTN_KEYS='[self_attn.q_proj, self_attn.k_proj, self_attn.v_proj, self_attn.o_proj, linear_attn.in_proj_qkv, linear_attn.in_proj_z, linear_attn.out_proj]'
SEQ=""; LAYERS=""; KEYS=""
for cfg in "3200 16 full" "2600 16 full" "2600 8 full" "2600 8 attn"; do
  set -- ${=cfg}; s=$1; l=$2; k=$3
  y=$D/memtest_${s}_${l}_${k}.yaml; log=$D/memtest_${s}_${l}_${k}.log
  sed -e "s/^max_seq_length:.*/max_seq_length: $s/" -e "s/^num_layers:.*/num_layers: $l/" $D/memtest.yaml > $y
  if [ $k = attn ]; then
    $PY - "$y" "$ATTN_KEYS" <<'EOF'
import sys, re
p, keys = sys.argv[1], sys.argv[2]
s = open(p).read()
s = re.sub(r"  keys: \[.*?\]\n", f"  keys: {keys}\n", s, flags=re.S)
open(p, "w").write(s)
EOF
  fi
  step "memtest seq $s, $l layers, $k keys"
  (cd $M && .venv/bin/python qwen35moe/lora_moe.py -c qwen35moe/$(basename $y)) > $log 2>&1
  rc=$?
  peak=$(grep -oE "Peak mem [0-9.]+ GB" $log | tail -1)
  if [ $rc -eq 0 ] && grep -q "Iter 30:" $log; then
    step "memtest PASS seq $s / $l layers / $k: $peak, $(grep -oE 'Tokens/sec [0-9.]+' $log | tail -1)"
    SEQ=$s; LAYERS=$l; KEYS=$k; break
  fi
  step "memtest FAIL seq $s / $l layers / $k (rc $rc, $peak): $(grep -iE 'error|abort|out of memory' $log | tail -1 | cut -c1-200)"
done
rm -rf $D/adapters_memtest
[ -n "$SEQ" ] || { step "STEP3 FAILED: no config fits; stopping (next fallback: sudo sysctl iogpu.wired_limit_mb=28672)"; exit 1; }

# ---- 4. speed test --------------------------------------------------------------------------------------------
cp $D/memtest_${SEQ}_${LAYERS}_${KEYS}.yaml $D/speed.yaml
sed -i '' -e "s#^data:.*#data: ./qwen35moe/data_mix$SEQ#" -e "s#^adapter_path:.*#adapter_path: ./qwen35moe/adapters_speed#" \
  -e "s/^iters:.*/iters: 200/" -e "s/^seed:.*/seed: 35/" -e "s/^steps_per_report:.*/steps_per_report: 10/" \
  -e "s/^steps_per_eval:.*/steps_per_eval: 200/" -e "s/^val_batches:.*/val_batches: 25/" -e "s/^save_every:.*/save_every: 50/" $D/speed.yaml
cat >> $D/speed.yaml <<'EOF'
lr_schedule:
  name: cosine_decay
  warmup: 10
  warmup_init: 1.0e-6
  arguments: [1.0e-4, 40, 1.0e-5]     # [init, decay_steps, end] (optimizer updates = 200 / 4)
EOF
if ! grep -q "^exit=0" $D/train_speed.log 2>/dev/null; then
  step "speed test: 200 micro-batches, seq $SEQ, $LAYERS layers, $KEYS keys"
  (echo "start $(date "+%Y-%m-%d %H:%M:%S %z")"
   cd $M && .venv/bin/python train_resumable.py --config qwen35moe/speed.yaml --launcher qwen35moe/lora_moe.py --max-restarts 10
   echo "exit=$?"; echo "end $(date "+%Y-%m-%d %H:%M:%S %z")") >> $D/train_speed.log 2>&1
fi
grep -q "^exit=0" $D/train_speed.log || { step "STEP4 FAILED (training)"; exit 1; }
step "speed: $(grep -E 'Iter [0-9]+: Train' $D/train_speed.log | tail -1)"
step "val: $(grep -E 'Val loss' $D/train_speed.log | tr '\n' ' ')"

# ---- 5. export + sign of life ---------------------------------------------------------------------------------
if [ ! -s $D/gguf/pilot-q4.gguf ]; then
  unload
  step "export: fuse (dequantized)"
  rm -rf $D/hf_pilot
  (cd $M && .venv/bin/mlx_lm.fuse --model $BASE --adapter-path qwen35moe/adapters_speed --save-path qwen35moe/hf_pilot --dequantize) || { step "fuse failed"; exit 1; }
  $PY $M/qwen35_pilot/fix_norms.py $D/hf_pilot || { step "fix_norms failed"; exit 1; }
  $PY $D/fix_experts.py $D/hf_pilot || { step "fix_experts failed"; exit 1; }
  step "convert -> f16 GGUF -> Q4_K_M"
  $PY llama.cpp/convert_hf_to_gguf.py $D/hf_pilot --outtype f16 --no-mtp --outfile $D/gguf/pilot-f16.gguf || { step "gguf failed"; exit 1; }
  llama.cpp/build/bin/llama-quantize $D/gguf/pilot-f16.gguf $D/gguf/pilot-q4.gguf Q4_K_M > $D/quantize_pilot.log 2>&1 || { step "quantize failed"; exit 1; }
  rm -rf $D/hf_pilot $D/gguf/pilot-f16.gguf
fi
sed 's#^FROM .*#FROM ./gguf/pilot-q4.gguf#' $M/qwen35_pilot/Modelfile.q35m6g > $D/Modelfile.pilot
(cd $D && ollama create sql2pyspark:$T -f Modelfile.pilot) || { step "ollama create failed"; exit 1; }
E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
step "$T holdout (62)";    E --set eval/holdout.jsonl    --out $R/${T}__holdout.json
step "$T holdout_v4 (25)"; E --set eval/holdout_v4.jsonl --out $R/${T}__holdout_v4.json
unload
step "STEP345 DONE (seq $SEQ, $LAYERS layers, $KEYS keys)"
