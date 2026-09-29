SELECT id, merchant, txn_time, amount, LEAD(amount, 2) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS next_amount FROM transactions;
