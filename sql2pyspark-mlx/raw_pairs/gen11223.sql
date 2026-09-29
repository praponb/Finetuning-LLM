SELECT
    id,
    txn_time,
    merchant,
    RANK() OVER (PARTITION BY TO_DATE(txn_time), merchant ORDER BY txn_time DESC) AS rnk
FROM transactions
