WITH recent_transactions AS (
    SELECT *
    FROM transactions
    WHERE amount < 3700
),
txn_month_summary AS (
    SELECT DATE_FORMAT(txn_time, 'yyyy-MM') AS txn_month, ROUND((100 * SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_shell
    FROM recent_transactions
    GROUP BY DATE_FORMAT(txn_time, 'yyyy-MM')
)
SELECT txn_month, pct_shell
FROM txn_month_summary
WHERE pct_shell >= 37
ORDER BY txn_month
LIMIT 50
