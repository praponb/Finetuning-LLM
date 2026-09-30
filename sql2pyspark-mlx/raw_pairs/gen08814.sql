SELECT id, txn_type, txn_time, amount, LAG(amount, 1) OVER (PARTITION BY txn_type ORDER BY txn_time, id) prev_amount FROM transactions WHERE merchant IN ('Amazon', '7-Eleven', 'Grab');
