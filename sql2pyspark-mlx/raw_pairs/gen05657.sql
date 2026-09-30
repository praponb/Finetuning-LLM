SELECT id, txn_type, txn_time, amount, LAG(amount, 1, 0) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS prev_amount FROM transactions;
