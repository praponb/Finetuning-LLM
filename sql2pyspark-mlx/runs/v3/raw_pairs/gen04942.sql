WITH recent_transactions AS (
    SELECT *, IF(is_flagged, 'Yes', 'No') is_flagged_label
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 6 AND 9
),
txn_day_summary AS (
    SELECT TO_DATE(txn_time) txn_day, MIN(amount) min_amount, AVG(amount) avg_amount
    FROM recent_transactions
    GROUP BY TO_DATE(txn_time)
)
SELECT txn_day, min_amount, avg_amount
FROM txn_day_summary
WHERE avg_amount < 3200
ORDER BY avg_amount, min_amount;
