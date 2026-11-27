#!/bin/zsh
# Qwen3.5-9B round "q9g" on the Mac mini M6 (owner 2026-11-26): one more full round CONTINUED FROM q9df3, no API spend.
#   1. train lora_config_q9g.yaml (9,600 micro-batches on data_q9g from adapters_q9g_init = q9df3 as one rank-96 LoRA;
#      auto-resume after Metal aborts) with a divergence guard (stops if the latest val loss > 3x the best and > 0.05)
#   2. candidates: q9g-q4 (fused) and q9gm-q4 (50/50 avg_weights of fused q9df3 + fused q9g), Q4_K_M, Ollama tags
#   3. quick gate each: smoke, star probe (loops), hold-out / v4 / fixed / dev, harness v2 (+ type-strict)
#   4. if neither passes but one raises harness v2 to >= 10/50: up to 3 more merges (q9g weight 0.35 / 0.25 / 0.15),
#      stop at the first that passes (owner 11-26)
#   5. full evals (12 sets, harness v1, real-style) on every passing candidate, best first; name slips
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUF files or the site. Promotion is the owner's call.
# Logs: qwen35_pilot/q9g/chain_q9g.log (run with >>), qwen35_pilot/train_q9g.log. Resumable: rerun to continue.
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
Q=$P/q9g; LOG=$P/train_q9g.log; CFG=qwen35_pilot/lora_config_q9g.yaml
H=$M/complex_v16/data/complex_shift_meta.jsonl; HS=$M/schema_v15/data/schema_shift_meta.jsonl
ensure_ollama() { until ollama list >/dev/null 2>&1; do sleep 2; done }

guard() {
  while kill -0 $1 2>/dev/null; do
    sleep 300
    local vals=$(grep -oE "Val loss [0-9.]+" $LOG | awk '{print $3}')
    [ $(echo "$vals" | grep -c .) -ge 3 ] || continue
    if echo "$vals" | awk 'NR==1{m=$1} {if($1<m)m=$1; l=$1} END{exit !(l > 3*m && l > 0.05)}'; then
      echo "DIVERGED $(date "+%F %T %z"): latest val loss > 3x best" >> $LOG
      pkill -f "train_resumable.py --config $CFG"; sleep 2; pkill -f "qwen35_pilot/lora_q35.py"
      return
    fi
  done
}

[ -s $P/data_q9g/train.jsonl ] && [ -s $P/data_q9g/valid.jsonl ] || { step "data missing; stopping"; exit 1; }
[ -x llama.cpp/build/bin/llama-quantize ] || { step "llama-quantize missing; stopping"; exit 1; }
[ -d $W/hf_q9df3 ] || { step "fused q9df3 ($W/hf_q9df3) missing; stopping"; exit 1; }
caffeinate -s -i -w $$ &

# 1. training (the site's model is unloaded; it reloads on the next site request)
if ! grep -q "^exit=0" $LOG 2>/dev/null; then
  grep -q "^DIVERGED" $LOG 2>/dev/null && { step "training diverged earlier; stopping (see $LOG)"; exit 1; }
  unload; step "training q9g: 9,600 micro-batches from q9df3 on data_q9g (lr 1e-5 -> 5e-7)"
  (echo "start $(date "+%F %T %z")"
   cd $M && .venv/bin/python train_resumable.py --config $CFG --launcher qwen35_pilot/lora_q35.py --max-restarts 40 --segment-val-batches 100
   echo "exit=$?"; echo "end $(date "+%F %T %z")") >> $LOG 2>&1 &
  TP=$!; guard $TP & GP=$!
  wait $TP; kill $GP 2>/dev/null
fi
grep -q "^DIVERGED" $LOG && { step "training DIVERGED; stopping"; exit 1; }
grep -q "^exit=0" $LOG || { step "training failed; stopping"; exit 1; }
step "training done: $(grep -E 'Val loss' $LOG | tail -1)"

# 2. candidates
unload
fuse_adapter qwen35_pilot/adapters_q9g $W/hf_q9g || { step "fuse q9g failed; stopping"; exit 1; }
# mk <tag> <q9g weight>: avg_weights of fused q9g and fused q9df3 (weight 1 = q9g itself) -> Q4_K_M -> tag
mk() {
  local T=$1 w=$2 hf=$W/hf_$1
  if [ ! -s $G/$T.gguf ]; then
    step "$T: q9g weight $w (rest q9df3) -> f16 -> Q4_K_M"
    if [ "$w" = 1 ]; then hf=$W/hf_q9g; else
      rm -rf $hf; $PY $V/avg_weights.py $hf $W/hf_q9g $W/hf_q9df3 --weights $w $((1 - w)) || return 1; fi
    to_f16 $hf $W/$T-f16.gguf || return 1
    [ "$hf" = "$W/hf_q9g" ] || rm -rf $hf
    quant $W/$T-f16.gguf $G/$T.gguf Q4_K_M || return 1
    rm -f $W/$T-f16.gguf
  fi
  ensure_ollama; tag $G/$T.gguf ${T#sql2pyspark:} || return 1
}
# gate <tag>: smoke, star probe, quick evals, harness v2 (+ strict)
gate() {
  local T=$1
  smoke $T || step "smoke $T failed"
  zsh $V/star_probe.sh $T
  quick_evals $T
  if [ ! -s eval/results/harness/${T}__harness_suite_v2.strict.json ]; then
    step "$T harness v2 (50)"
    $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 --num-predict 4096 --set eval/harness_suite_v2.jsonl \
      --tables eval/harness_tables_v2.py --out eval/results/harness/${T}__harness_suite_v2.json > $R/${T}__harness_v2.log 2>&1
    $PY eval/harness_strict.py --v2 eval/results/harness/${T}__harness_suite_v2.json >> $R/${T}__harness_v2.log 2>&1
  fi
  unload
}
TAGS=(q9g-q4 q9gm-q4)
mk q9g-q4 1 || { step "export q9g-q4 failed; stopping"; exit 1; }
mk q9gm-q4 0.5 || { step "export q9gm-q4 failed; stopping"; exit 1; }
for t in $TAGS; do gate $t; done
python3 $V/gate_q9g.py v18a-q4 q9df3-q4 $TAGS
PASS=($(python3 $V/gate_q9g.py --all $TAGS))
step "passing: ${PASS:-none}"

# 4. extra merges (owner 11-26: up to 3) when nothing passes but harness v2 rose a lot
if [ ${#PASS} = 0 ]; then
  best=0; for t in $TAGS; do h=$(python3 $V/gate_q9g.py --harness $t); (( h > best )) && best=$h; done
  if (( best >= 10 )); then
    for w in 0.35 0.25 0.15; do
      T=q9gw${w#0.}-q4
      mk $T $w || { step "export $T failed"; continue; }
      gate $T; TAGS+=($T)
      python3 $V/gate_q9g.py v18a-q4 q9df3-q4 $TAGS
      PASS=($(python3 $V/gate_q9g.py --all $T))
      [ ${#PASS} -gt 0 ] && break
    done
  else
    step "harness v2 best $best/50 < 10: no extra merges"
  fi
fi
[ ${#PASS} = 0 ] && { step "Q9G DONE: no candidate passes the quick gate"; exit 0; }

# 5. full evals on every passing candidate, best first
full_evals() {
  local T=$1
  [ -s eval/results/complex-shift/${T}__complex_shift.json ] && { step "$T full evals already done"; return 0; }
  step "full evals: $T"
  for s in holdout holdout_v4 test_fixed; do
    [ -s eval/results/${T}__$s.json ] || cp $R/${T}__$s.json eval/results/${T}__$s.json
  done
  E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
  [ -s eval/results/${T}__real_queries.json ] || { step "$T real_queries"; E --set eval/real_queries.jsonl --tables eval/real_tables.py; }
  step "$T complex_v8 test (100 of 300)"
  E --set eval/complex_v8_test.jsonl --tables $M/complex_v8/eval_tables.py --sample 100 --seed 0
  step "$T IN / NOT IN held-out pairs (50)"
  E --set $M/classic_v10/data/inlist_valid_meta.jsonl --out eval/results/inlist/${T}__inlist_valid.json
  for v in 11 12 13 14; do
    step "$T targeted_v$v held-out pairs"
    E --set $M/targeted_v$v/data/heldout_meta.jsonl --out eval/results/v$v-heldout/${T}__heldout.json
  done
  step "$T schema-shift pairs (200)"
  E --set $HS --tables eval/schema_shift_tables.py --out eval/results/schema-shift/${T}__schema_shift.json
  step "$T complex schema-shift pairs (150 of 300)"
  E --set $H --sample 150 --seed 0 --tables eval/complex_shift_tables.py --out eval/results/complex-shift/${T}__complex_shift.json
  unload
  step "$T harness_suite (21)"
  E --num-predict 4096 --set eval/harness_suite.jsonl --tables eval/harness_tables.py --out eval/results/harness/${T}__harness_suite.json
  $PY eval/harness_strict.py eval/results/harness/${T}__harness_suite.json
  unload
  step "$T real-style pairs (74)"
  $PY $M/realstyle_v19/eval_realstyle.py --model sql2pyspark:$T --think false
  unload
}
for t in $PASS; do full_evals $t; done
step "name slips"; python3 eval/name_slips.py --models v18a-q4 q9df3-q4 $PASS
python3 $V/gate_q9g.py v18a-q4 q9df3-q4 $TAGS
step "Q9G DONE (full evals: $PASS)"
