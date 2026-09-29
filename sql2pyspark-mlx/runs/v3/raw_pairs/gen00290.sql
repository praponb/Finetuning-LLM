WITH ranked AS (
    SELECT
        id,
        merchant,
        txn_time,
        is_flagged,
        ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time, id) AS rnk
    FROM transactions
)
SELECT
    id,
    merchant,
    txn_time,
    is_flagged
FROM ranked
WHERE rnk = 1
