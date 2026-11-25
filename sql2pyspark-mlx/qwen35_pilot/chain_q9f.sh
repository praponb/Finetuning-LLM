#!/bin/zsh
# Step D of the 11-23 improvement plan: ORPO preference round "q9f" on top of an existing adapter.
#   usage: zsh chain_q9f.sh <resume adapter dir, e.g. qwen35_pilot/adapters_q9e> <precision: q4 | q4im | q6 | q8>
#   0. waits for step_c.sh (data_q9f/pref_*.jsonl)
#   1. sanity run: 20 pairs, 40 micro-batches at lr 2e-5, validated on the same 20 pairs -> loss must fall, margin rise,
#      peak memory <= 17 GB; otherwise stop
#   2. training (pref_q35.py) with restarts after Metal aborts (max 40) and a divergence guard on the replay loss
#   3. export at the chosen precision -> sql2pyspark:q9f-<prec>   4. dev set + the 12 eval sets   5. name slips
#   6. harness 21   7. real-style 74
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUFs or the site. Log: qwen35_pilot/chain_q9f.log
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
RES=${1:?adapter dir}; PREC=${2:?precision}
case $PREC in q4) QT=Q4_K_M; IM= ;; q4im) QT=Q4_K_M; IM=$V/q9e.imatrix ;; q6) QT=Q6_K; IM= ;; q8) QT=Q8_0; IM= ;;
  *) echo "bad precision"; exit 1 ;; esac
T=q9f-$PREC; D=$P/data_q9f; LOG=$P/train_q9f.log
H=$M/complex_v16/data/complex_shift_meta.jsonl; HS=$M/schema_v15/data/schema_shift_meta.jsonl
while pgrep -f "selfplay/step_c.sh" > /dev/null; do sleep 60; done
[ -s $D/pref_train.jsonl ] && [ -s $D/pref_valid.jsonl ] || { step "preference data missing; stopping"; exit 1; }
caffeinate -s -i -w $$ &
PREF=(.venv/bin/python qwen35_pilot/pref_q35.py --resume-adapter-dir ${RES#$M/} --replay qwen35_pilot/data_q9e/train.jsonl)

# 1. sanity run
if ! grep -q "SANITY OK" $P/sanity_q9f.log 2>/dev/null; then
  unload; step "sanity run (20 pairs)"
  mkdir -p $D/sanity; head -20 $D/pref_train.jsonl > $D/sanity/pref_train.jsonl; cp $D/sanity/pref_train.jsonl $D/sanity/pref_valid.jsonl
  rm -rf $P/adapters_q9f_sanity
  (cd $M && "${PREF[@]}" --out qwen35_pilot/adapters_q9f_sanity --data qwen35_pilot/data_q9f/sanity --epochs 2 \
     --max-iters 40 --lr 2e-5 --warmup 2 --report-every 10 --save-every 1000 --eval-every 1000) > $P/sanity_q9f.log 2>&1 \
     || { step "sanity run crashed; stopping (see sanity_q9f.log)"; exit 1; }
  $PY - $P/sanity_q9f.log <<'EOF' >> $P/sanity_q9f.log || { step "sanity check failed; stopping (see sanity_q9f.log)"; exit 1; }
import ast, re, sys
t = open(sys.argv[1]).read()
v0 = ast.literal_eval(re.search(r"val at 0: (\{.*\})", t).group(1))
v1 = ast.literal_eval(re.search(r"val at end: (\{.*\})", t).group(1))
peak = max(float(x) for x in re.findall(r"peak ([0-9.]+) GB", t))
ok = v1["margin"] > v0["margin"] and v1["nll_chosen"] <= v0["nll_chosen"] * 1.05 and peak <= 17.0
print(f"sanity: margin {v0['margin']} -> {v1['margin']}, nll_chosen {v0['nll_chosen']} -> {v1['nll_chosen']}, "
      f"acc {v0['acc']} -> {v1['acc']}, peak {peak} GB -> {'SANITY OK' if ok else 'SANITY FAIL'}")
sys.exit(0 if ok else 1)
EOF
  rm -rf $P/adapters_q9f_sanity
fi
step "$(grep -E 'SANITY' $P/sanity_q9f.log | tail -1)"

# 2. training with restarts + guard (stop if the mean replay loss of a report > 3x the first report's and > 0.05)
guard() {
  while kill -0 $1 2>/dev/null; do
    sleep 300
    local v=$(grep -oE "replay loss [0-9.]+" $LOG | awk '{print $3}' | awk '$1>0')
    [ $(echo "$v" | grep -c .) -ge 4 ] || continue
    if echo "$v" | awk 'NR==1{f=$1} {l=$1} END{exit !(l > 3*f && l > 0.05)}'; then
      echo "DIVERGED $(date "+%F %T %z"): replay loss > 3x the first report" >> $LOG; pkill -f "pref_q35.py --resume"; return
    fi
  done
}
if ! grep -q "^\[pref\] finished" $LOG 2>/dev/null; then
  grep -q "^DIVERGED" $LOG 2>/dev/null && { step "training diverged earlier; stopping"; exit 1; }
  unload; step "training q9f (ORPO, lam 0.1, lr 2e-6 -> 2e-7) from $RES"
  for r in {0..40}; do
    (cd $M && "${PREF[@]}" --out qwen35_pilot/adapters_q9f --data qwen35_pilot/data_q9f) >> $LOG 2>&1 &
    TP=$!; guard $TP & GP=$!; wait $TP; kill $GP 2>/dev/null
    grep -q "^DIVERGED" $LOG && { step "training DIVERGED; stopping"; exit 1; }
    grep -q "^\[pref\] finished" $LOG && break
    echo "[restart $((r + 1)) $(date "+%F %T %z")]" >> $LOG; sleep 20
  done
fi
grep -q "^\[pref\] finished" $LOG || { step "training failed (restart limit); stopping"; exit 1; }
step "training done: $(grep -E 'val at end' $LOG | tail -1)"

# 3. export
if [ ! -s $G/$T.gguf ]; then
  unload; step "export q9f -> $QT"
  fuse_adapter qwen35_pilot/adapters_q9f $W/hf_q9f || { step "fuse failed; stopping"; exit 1; }
  to_f16 $W/hf_q9f $W/q9f-f16.gguf || { step "f16 failed; stopping"; exit 1; }
  rm -rf $W/hf_q9f
  quant $W/q9f-f16.gguf $G/$T.gguf $QT $IM || { step "quantize failed; stopping"; exit 1; }
  rm -f $W/q9f-f16.gguf
fi
tag $G/$T.gguf $T || { step "ollama create failed; stopping"; exit 1; }
smoke $T || { step "smoke failed; stopping"; exit 1; }

# 4. dev set + the 12 eval sets (default result paths, as for q9e)
E() { $PY eval/evaluate.py --model sql2pyspark:$T --think false --timeout 600 "$@"; }
step "$T dev_short";    E --set eval/dev_short.jsonl --out $R/${T}__dev_short.json
step "$T holdout";      E --set eval/holdout.jsonl
step "$T holdout_v4";   E --set eval/holdout_v4.jsonl
step "$T real_queries"; E --set eval/real_queries.jsonl --tables eval/real_tables.py
step "$T test_fixed";   E --set eval/test_fixed.jsonl
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

# 5-7
step "name slips"; python3 eval/name_slips.py --models v18a-q4 q9e-q4 $T
unload
step "$T harness_suite (21)"
E --num-predict 4096 --set eval/harness_suite.jsonl --tables eval/harness_tables.py --out eval/results/harness/${T}__harness_suite.json
$PY eval/harness_strict.py eval/results/harness/${T}__harness_suite.json
unload
step "$T real-style pairs (74)"
$PY $M/realstyle_v19/eval_realstyle.py --model sql2pyspark:$T --think false
unload
step "ALL DONE"
