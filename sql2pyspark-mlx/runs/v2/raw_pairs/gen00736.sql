SELECT id, account_id, txn_time, amount, amount - LAG(amount, 1) OVER (PARTITION BY account_id ORDER BY txn_time, id) AS amount_change FROM transactions
