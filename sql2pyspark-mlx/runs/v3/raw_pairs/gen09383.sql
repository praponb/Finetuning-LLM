WITH ranked_transactions AS (
    SELECT *, RANK() OVER (PARTITION BY merchant ORDER BY txn_time DESC) rn
    FROM transactions
)
SELECT *
FROM ranked_transactions
WHERE rn = 1
