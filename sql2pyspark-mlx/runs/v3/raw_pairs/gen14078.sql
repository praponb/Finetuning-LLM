WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS row_num
    FROM transactions
)
SELECT
    id,
    txn_type,
    txn_time,
    merchant
FROM ranked
WHERE row_num <= 3
ORDER BY txn_time ASC, id DESC;
