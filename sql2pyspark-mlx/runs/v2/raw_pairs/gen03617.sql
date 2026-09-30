SELECT id, txn_type, txn_time, amount, SUM(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS running_amount FROM transactions
