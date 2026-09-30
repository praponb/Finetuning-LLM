WITH filtered_transactions AS (
    SELECT *
    FROM transactions
    WHERE TO_DATE(txn_time) < '2025-07-01' AND NOT is_flagged
),
summary AS (
    SELECT txn_type, is_flagged, ROUND(AVG(amount), 2) AS avg_amount
    FROM filtered_transactions
    GROUP BY txn_type, is_flagged
)
SELECT txn_type, is_flagged, avg_amount
FROM summary
WHERE avg_amount >= 27
ORDER BY txn_type, is_flagged
LIMIT 50;
