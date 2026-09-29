WITH Src AS (
    SELECT *, CASE WHEN amount >= 4400 THEN 'High' WHEN amount >= 600 THEN 'Medium' ELSE 'Low' END AS amount_tier
    FROM transactions
    WHERE merchant NOT IN ('Lazada', 'Starbucks', 'Shell')
),
transactions_stats AS (
    SELECT merchant, MIN(txn_time) AS min_txn_time
    FROM Src
    GROUP BY merchant
)
SELECT *
FROM transactions_stats
ORDER BY min_txn_time, merchant
LIMIT 5;
