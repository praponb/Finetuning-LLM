WITH Base AS (
    SELECT *
    FROM transactions
    WHERE NOT (is_flagged AND is_flagged = TRUE)
),
transactions_stats AS (
    SELECT merchant, MAX(txn_time) AS last_txn_time, ROUND(AVG(amount), 2) AS avg_amount
    FROM Base
    GROUP BY merchant
)
SELECT merchant, avg_amount
FROM transactions_stats
WHERE avg_amount > 60;
