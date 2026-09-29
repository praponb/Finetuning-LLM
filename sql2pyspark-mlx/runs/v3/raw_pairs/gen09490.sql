SELECT id, txn_type, txn_time, amount, LEAD(amount, 1) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS next_amount FROM transactions WHERE is_flagged ORDER BY txn_type, txn_time
