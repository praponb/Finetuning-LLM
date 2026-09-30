SELECT id, txn_time, amount, amount - LAG(amount, 1) OVER (ORDER BY txn_time, id) AS amount_change FROM transactions ORDER BY amount_change, amount DESC
