WITH ranked AS (
    SELECT
        id,
        txn_type,
        txn_time,
        amount,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS row_num
    FROM transactions
)
SELECT
    id,
    txn_type,
    txn_time,
    amount
FROM ranked
WHERE row_num <= 5
