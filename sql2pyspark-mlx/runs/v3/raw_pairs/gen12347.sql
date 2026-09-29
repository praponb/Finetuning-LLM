SELECT id, txn_time, amount, amount - LAG(amount, 1, 0) OVER (ORDER BY txn_time, id) AS amount_change FROM transactions WHERE txn_time < '2023-07-14'
