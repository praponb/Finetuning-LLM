#!/bin/zsh
# Fix round for q9df's `SELECT * ... [NOT] IN / [NOT] EXISTS (subquery)` loops (owner chose option A, 11-25 ~06:45).
#   1. collect q9df-q4's answers on selfplay/star_pool.jsonl (400 Spark-verified pairs, star_gen.py), greedy,
#      num_predict 1024 (bounds a loop) -> selfplay/raw_star_q9df-q4.jsonl
#   2. preference pairs (rejected = q9df's wrong / looping answer, chosen = its own right answer or the reference)
#      -> data_q9f2/
#   3. ORPO round q9f2 from the q9f adapter (pref_q35.py, lr 3e-6 -> 3e-7, replay 1:2 from data_q9e), restarts + guard
#   4. q9df2-q4  = 0.5 q9d + 1.0 q9f2 - 0.5 q9f   (= q9df + the whole fix delta)
#      q9df2h-q4 = 0.5 q9d + 0.5 q9f2             (= q9df + half the fix delta, fallback)
#   5. star probe (30) for both, then hold-out / v4 / fixed / dev for both -> eval/results/variants/
#   Stops there ("Q9F2 QUICK DONE"): the full evals are started after a look at these numbers.
# Never touches sql2pyspark:latest / v18a-q4 / v15-q4, the root GGUFs or the site. Log: qwen35_pilot/chain_q9f2.log
cd /Users/praponb/GitHub/Finetuning-LLM
source sql2pyspark-mlx/qwen35_pilot/variants/export_lib.sh
S=$P/selfplay; D=$P/data_q9f2; LOG=$P/train_q9f2.log; RAW=$S/raw_star_q9df-q4.jsonl
while pgrep -f "variants/star_probe.sh" > /dev/null; do sleep 30; done
caffeinate -s -i -w $$ &

# 1. collect
step "collect q9df-q4 on the star pool (400)"
n=0
until grep -q "COLLECT DONE" $S/collect_star_q9df-q4.log 2>/dev/null; do
  $PY $S/collect.py --model sql2pyspark:q9df-q4 --pool $S/star_pool.jsonl --out $RAW --num-predict 1024 --temp-k 0 \
    >> $S/collect_star_q9df-q4.log 2>&1 || { step "collect exited; retry in 60 s"; sleep 60; }
  n=$((n + 1)); [ $n -gt 10 ] && { step "collect failed 10 times; stopping"; exit 1; }
done
unload

# 2. pairs
step "preference pairs"
$PY $S/make_pref.py --raw $RAW --pool $S/star_pool.jsonl --out $D --max-rejected 1 || { step "make_pref failed; stopping"; exit 1; }
[ $(wc -l < $D/pref_train.jsonl) -ge 30 ] || { step "fewer than 30 pairs; stopping"; exit 1; }

# 3. training with restarts + guard (stop if the mean replay loss of a report > 3x the first report's and > 0.05)
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
PREF=(.venv/bin/python qwen35_pilot/pref_q35.py --resume-adapter-dir qwen35_pilot/adapters_q9f --replay qwen35_pilot/data_q9e/train.jsonl
      --replay-ratio 2 --lr 3e-6 --lr-end 3e-7 --seed 50)
if ! grep -q "^\[pref\] finished" $LOG 2>/dev/null; then
  grep -q "^DIVERGED" $LOG 2>/dev/null && { step "training diverged earlier; stopping"; exit 1; }
  step "training q9f2 (ORPO, lam 0.1, lr 3e-6 -> 3e-7) from adapters_q9f"
  for r in {0..40}; do
    (cd $M && "${PREF[@]}" --out qwen35_pilot/adapters_q9f2 --data qwen35_pilot/data_q9f2) >> $LOG 2>&1 &
    TP=$!; guard $TP & GP=$!; wait $TP; kill $GP 2>/dev/null
    grep -q "^DIVERGED" $LOG && { step "training DIVERGED; stopping"; exit 1; }
    grep -q "^\[pref\] finished" $LOG && break
    echo "[restart $((r + 1)) $(date "+%F %T %z")]" >> $LOG; sleep 20
  done
fi
grep -q "^\[pref\] finished" $LOG || { step "training failed (restart limit); stopping"; exit 1; }
step "training done: $(grep -E 'val at end' $LOG | tail -1)"

# 4. merges
if [ ! -s $G/q9df2-q4.gguf ] || [ ! -s $G/q9df2h-q4.gguf ]; then
  unload; step "fuse q9d, q9f, q9f2"
  fuse_adapter qwen35_pilot/adapters_q9d $W/hf_q9d || { step "fuse q9d failed; stopping"; exit 1; }
  fuse_adapter qwen35_pilot/adapters_q9f $W/hf_q9f || { step "fuse q9f failed; stopping"; exit 1; }
  fuse_adapter qwen35_pilot/adapters_q9f2 $W/hf_q9f2 || { step "fuse q9f2 failed; stopping"; exit 1; }
  for spec in "q9df2:0.5 1.0 -0.5:hf_q9d hf_q9f2 hf_q9f" "q9df2h:0.5 0.5:hf_q9d hf_q9f2"; do
    name=${spec%%:*}; rest=${spec#*:}; wts=${rest%%:*}; ins=${rest#*:}; t=$name-q4
    [ -s $G/$t.gguf ] && continue
    step "$t: weights $wts over $ins -> f16 -> Q4_K_M"
    rm -rf $W/hf_$name
    inputs=(); for x in ${=ins}; do inputs+=($W/$x); done
    $PY $V/avg_weights.py $W/hf_$name $inputs --weights ${=wts} || { step "merge $name failed; stopping"; exit 1; }
    to_f16 $W/hf_$name $W/$name-f16.gguf || { step "f16 $name failed; stopping"; exit 1; }
    rm -rf $W/hf_$name
    quant $W/$name-f16.gguf $G/$t.gguf Q4_K_M || { step "quant $t failed; stopping"; exit 1; }
    rm -f $W/$name-f16.gguf
  done
  rm -rf $W/hf_q9d $W/hf_q9f $W/hf_q9f2   # my working folders
fi
for t in q9df2-q4 q9df2h-q4; do
  tag $G/$t.gguf $t || { step "ollama create $t failed; stopping"; exit 1; }
  smoke $t || step "smoke $t failed"
done

# 5. probe + quick evals
zsh $V/star_probe.sh q9df2-q4 q9df2h-q4
for t in q9df2-q4 q9df2h-q4; do quick_evals $t; done
step "Q9F2 QUICK DONE"
