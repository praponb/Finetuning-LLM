SELECT id, txn_time, amount, LAG(amount, 1, 0) OVER (ORDER BY txn_time, id) AS prev_amount FROM transactions;
