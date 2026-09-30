SELECT MAX(txn_time) last_txn_time, MAX(amount) max_amount, COUNT(DISTINCT amount) unique_txn_types FROM transactions ORDER BY max_amount DESC;
