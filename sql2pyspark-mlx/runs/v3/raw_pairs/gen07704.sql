SELECT
    id,
    merchant,
    txn_time,
    RANK() OVER (PARTITION BY merchant ORDER BY txn_time) AS rnk
FROM transactions
WHERE merchant IN ('7-Eleven', 'Lazada', 'Amazon')
