SELECT id, account_id, txn_time, amount, ROUND(AVG(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3 FROM transactions
