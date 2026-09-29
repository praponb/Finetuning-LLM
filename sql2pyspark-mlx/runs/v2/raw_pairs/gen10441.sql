SELECT
    id,
    account_id,
    merchant,
    txn_time,
    amount,
    txn_type,
    ROUND(AVG(amount) OVER (PARTITION BY account_id, merchant ORDER BY txn_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS amount_moving_avg_7,
    LAST_VALUE(txn_type) OVER (ORDER BY txn_time DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_txn_type
FROM transactions
WHERE txn_time < '2025-11-01';
