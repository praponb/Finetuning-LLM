SELECT merchant, MAX(txn_time) last_txn_time, SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END) refund_count
FROM transactions
GROUP BY merchant;
