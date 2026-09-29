SELECT
    id,
    txn_type,
    txn_time,
    amount,
    merchant,
    ROUND(AVG(amount) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) amount_moving_avg_4,
    SUM(amount) OVER (PARTITION BY merchant) merchant_sum_amount
FROM transactions
ORDER BY amount_moving_avg_4 DESC;
