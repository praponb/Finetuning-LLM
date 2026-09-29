SELECT id, txn_time, amount, SUM(amount) OVER (ORDER BY txn_time, id) running_amount FROM transactions WHERE amount >= 1500;
