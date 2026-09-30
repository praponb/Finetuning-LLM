SELECT
    id,
    merchant,
    txn_time,
    NTILE(4) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS quartile
FROM transactions
WHERE txn_time >= '2025-07-18';
