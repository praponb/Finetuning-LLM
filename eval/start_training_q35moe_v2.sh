#!/bin/zsh
# Qwen3.5-35B-A3B ROUND 2 (user 2026-11-16 ~01:30): continue q35moe-v1 -> q35moe-v2-q4 (lora_v2.yaml), same export + evals.
#   1. train sql2pyspark-mlx/qwen35moe/lora_v2.yaml (4,800 micro-batches, seq 2600, 16 layers, rank 16) with the MoE
#      launcher (gd_chunked + chunked_loss + stop_gradient on the expert indices); auto-resume after Metal aborts
#   2. export: fuse --dequantize -> fix_norms -> fix_experts -> f16 GGUF (--no-mtp) -> Q4_K_M -> sql2pyspark:q35moe-v1-q4
#   3. the same 12 eval sets as v18 (Ollama, --think false)
#   4. the Kimi real-style pairs (74, own tables per case) for q35moe-v1-q4 and the live v18a-q4
#   5. name slips (v18a-q4 vs q35moe-v1-q4)
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4 or the root GGUF files. Promotion is left to the owner.
# Logs: eval/start_training_q35moe.log (this script), sql2pyspark-mlx/qwen35moe/train_v1.log. Resumable: rerun to continue.
cd /Users/praponb/GitHub/Finetuning-LLM
M=sql2pyspark-mlx; D=$M/qwen35moe; PY=$M/.venv/bin/python; BASE=mlx-community/Qwen3.5-35B-A3B-4bit
T=q35moe-v2-q4; LOG=$D/train_v2.log
H=$M/complex_v16/data/complex_shift_meta.jsonl
HS=$M/schema_v15/data/schema_shift_meta.jsonl
H11=$M/targeted_v11/data/heldout_meta.jsonl
H12=$M/targeted_v12/data/heldout_meta.jsonl
H13=$M/targeted_v13/data/heldout_meta.jsonl
H14=$M/targeted_v14/data/heldout_meta.jsonl
export HF_HUB_OFFLINE=1
step() { echo "[$(date +%H:%M:%S)] $*"; }
unload() { for m in $(ollama ps 2>/dev/null | awk 'NR>1{print $1}'); do ollama stop $m; done }
ensure_ollama() { until ollama list >/dev/null 2>&1; do sleep 2; done }

[ -s $D/data_mix2600/train.jsonl ] && [ -s $D/data_mix2600/valid.jsonl ] && [ -s $H ] || { step "data missing; stopping"; exit 1; }
[ -x llama.cpp/build/bin/llama-quantize ] || { step "llama-quantize missing; stopping"; exit 1; }
caffeinate -s -i -w $$ &
step "data ok: $(wc -l < $D/data_mix2600/train.jsonl) train / $(wc -l < $D/data_mix2600/valid.jsonl) valid"

# 1. training (the site's model is unloaded; it reloads on the next site request)
unload
if ! grep -q "^exit=0" $LOG 2>/dev/null; then
  step "start training q35moe-v2 (4,800 micro-batches)"
  (echo "start $(date "+%Y-%m-%d %H:%M:%S %z")"
   cd $M && .venv/bin/python train_resumable.py --config qwen35moe/lora_v2.yaml --launcher qwen35moe/lora_moe.py --max-restarts 20
   echo "exit=$?"; echo "end $(date "+%Y-%m-%d %H:%M:%S %z")") >> $LOG 2>&1
fi
grep -q "^exit=0" $LOG || { step "training failed; stopping"; exit 1; }
[ -s $D/adapters_v2/adapters.safetensors ] || { step "no adapters; stopping"; exit 1; }
step "training done: $(grep -E 'Val loss' $LOG | tail -1)"

# 2. export
if [ ! -s $D/gguf/v2-q4.gguf ]; then
  unload
  step "export: fuse (dequantized) -> fix_norms -> fix_experts"
  rm -rf $D/hf_v2
  (cd $M && .venv/bin/mlx_lm.fuse --model $BASE --adapter-path qwen35moe/adapters_v2 --save-path qwen35moe/hf_v2 --dequantize) \
    || { step "fuse failed"; exit 1; }
  $PY $M/qwen35_pilot/fix_norms.py $D/hf_v2 || { step "fix_norms failed"; exit 1; }
  $PY $D/fix_experts.py $D/hf_v2 || { step "fix_experts failed"; exit 1; }
  step "convert -> f16 GGUF -> Q4_K_M"
  $PY llama.cpp/convert_hf_to_gguf.py $D/hf_v2 --outtype f16 --no-mtp --outfile $D/gguf/v2-f16.gguf || { step "gguf failed"; exit 1; }
  llama.cpp/build/bin/llama-quantize $D/gguf/v2-f16.gguf $D/gguf/v2-q4.gguf Q4_K_M > $D/quantize_v2.log 2>&1 || { step "quantize failed"; exit 1; }
  rm -rf $D/hf_v2 $D/gguf/v2-f16.gguf
fi
ensure_ollama
if ! ollama list | grep -q "^sql2pyspark:$T "; then
  sed 's#^FROM .*#FROM ./gguf/v2-q4.gguf#' $M/qwen35_pilot/Modelfile.q35m6g > $D/Modelfile.v2
  (cd $D && ollama create sql2pyspark:$T -f Modelfile.v2) || { step "ollama create failed"; exit 1; }
fi

# 3. the 12 eval sets (default output paths, as for v18)
E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
step "$T holdout";      E --set eval/holdout.jsonl
step "$T holdout_v4";   E --set eval/holdout_v4.jsonl
step "$T real_queries"; E --set eval/real_queries.jsonl --tables eval/real_tables.py
step "$T test_fixed";   E --set eval/test_fixed.jsonl
step "$T complex_v8 test (100 of 300)"
E --set eval/complex_v8_test.jsonl --tables $M/complex_v8/eval_tables.py --sample 100 --seed 0
step "$T IN / NOT IN held-out pairs (50)"
E --set $M/classic_v10/data/inlist_valid_meta.jsonl --out eval/results/inlist/${T}__inlist_valid.json
step "$T targeted_v11 held-out pairs (300)"; E --set $H11 --out eval/results/v11-heldout/${T}__heldout.json
step "$T targeted_v12 held-out pairs (250)"; E --set $H12 --out eval/results/v12-heldout/${T}__heldout.json
step "$T targeted_v13 held-out pairs (150)"; E --set $H13 --out eval/results/v13-heldout/${T}__heldout.json
step "$T targeted_v14 held-out pairs (200)"; E --set $H14 --out eval/results/v14-heldout/${T}__heldout.json
step "$T schema-shift pairs (200, v15 set)"
E --set $HS --tables eval/schema_shift_tables.py --out eval/results/schema-shift/${T}__schema_shift.json
step "$T complex schema-shift pairs (150 of 300, unseen domains)"
E --set $H --sample 150 --seed 0 --tables eval/complex_shift_tables.py --out eval/results/complex-shift/${T}__complex_shift.json

# 4. real-style pairs (Kimi pilot, 74)
step "$T real-style pairs (74)"
$PY $D/eval_realstyle.py --model sql2pyspark:$T --think false
unload
if [ ! -s eval/results/realstyle/v18a-q4__realstyle.json ]; then
  step "v18a-q4 real-style pairs (74)"
  $PY $D/eval_realstyle.py --model sql2pyspark:v18a-q4
  unload
fi

# 5. name slips
step "name slips"; python3 eval/name_slips.py --models v18a-q4 q35moe-v1-q4 $T
step "ALL DONE"
