WITH ranked_transactions AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY merchant ORDER BY txn_time DESC) AS row_num
    FROM transactions
)
SELECT id, merchant, txn_time
FROM ranked_transactions
WHERE row_num = 1
