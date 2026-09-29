SELECT txn_type, txn_time, CAST(is_flagged AS INT) AS is_flagged_flag FROM transactions WHERE amount < 3000 AND txn_type = 'REFUND' ORDER BY txn_type DESC, txn_time;
