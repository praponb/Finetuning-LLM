SELECT id, merchant, txn_time, amount, LEAD(amount, 1) OVER (PARTITION BY merchant ORDER BY txn_time, id) next_amount FROM transactions WHERE HOUR(txn_time) BETWEEN 6 AND 13
