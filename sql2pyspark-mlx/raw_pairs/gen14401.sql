WITH top_transactions AS (
    SELECT *, RANK() OVER (PARTITION BY account_id ORDER BY txn_time DESC) AS rn
    FROM transactions
)
SELECT *
FROM top_transactions
WHERE rn <= 2
