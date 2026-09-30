SELECT
    id,
    merchant,
    amount,
    account_id,
    txn_type,
    txn_time,
    AVG(amount) OVER (PARTITION BY merchant) AS merchant_avg_amount,
    ROUND(AVG(amount) OVER (PARTITION BY account_id, txn_type ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_3
FROM transactions
WHERE TO_DATE(txn_time) >= '2025-09-26';
