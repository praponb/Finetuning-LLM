WITH ranked_transactions AS (
    SELECT
        id,
        txn_type,
        txn_time,
        merchant,
        DENSE_RANK() OVER (PARTITION BY txn_type ORDER BY txn_time DESC) AS rnk
    FROM transactions
)
SELECT *
FROM ranked_transactions
WHERE rnk <= 5
ORDER BY merchant
