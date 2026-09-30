WITH numbered AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS row_num
    FROM transactions
)
SELECT *
FROM numbered
WHERE row_num <= 3
ORDER BY 4;
