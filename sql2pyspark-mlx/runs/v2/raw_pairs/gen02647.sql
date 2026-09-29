WITH top_transactions AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS pos
    FROM transactions
)
SELECT *
FROM top_transactions
WHERE pos <= 3
