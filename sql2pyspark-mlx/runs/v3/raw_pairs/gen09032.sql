SELECT id, txn_time, amount, LEAD(amount, 2, 0) OVER (ORDER BY txn_time, id) next_amount FROM transactions WHERE account_id IN (4, 5, 25, 33)
