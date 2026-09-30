SELECT
    id,
    txn_type,
    amount,
    txn_time,
    ROUND(amount / SUM(amount) OVER (PARTITION BY txn_type), 3) AS amount_share,
    NTILE(3) OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS bucket
FROM transactions
