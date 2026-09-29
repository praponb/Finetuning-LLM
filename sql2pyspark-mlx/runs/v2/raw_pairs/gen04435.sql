SELECT id, txn_time, amount, LEAD(amount, 1) OVER (ORDER BY txn_time, id) next_amount FROM transactions
