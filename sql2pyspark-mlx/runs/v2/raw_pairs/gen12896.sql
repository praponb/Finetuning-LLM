SELECT
    id,
    txn_type,
    txn_time,
    amount,
    ROUND(AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_4
FROM transactions
WHERE txn_type = 'CREDIT';
