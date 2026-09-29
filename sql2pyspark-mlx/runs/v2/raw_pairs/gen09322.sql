WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE merchant = 'Starbucks'
),
transactions_stats AS (
    SELECT CASE WHEN amount >= 4400 THEN 'high' WHEN amount >= 600 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(DISTINCT amount) AS unique_txn_types
    FROM recent_transactions
    GROUP BY CASE WHEN amount >= 4400 THEN 'high' WHEN amount >= 600 THEN 'mid' ELSE 'low' END
)
SELECT amount_bucket, unique_txn_types
FROM transactions_stats
WHERE unique_txn_types > 6
ORDER BY unique_txn_types DESC
