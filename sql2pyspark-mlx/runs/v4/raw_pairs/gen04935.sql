SELECT
    id,
    merchant,
    txn_time,
    amount,
    ROUND(AVG(amount) OVER (PARTITION BY merchant ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3
FROM transactions
ORDER BY amount
