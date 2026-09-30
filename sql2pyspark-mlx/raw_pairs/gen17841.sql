WITH account_summary AS (
    SELECT account_id, ROUND(AVG(amount), 2) AS avg_amount, SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count
    FROM transactions
    WHERE HOUR(txn_time) BETWEEN 11 AND 16
    GROUP BY account_id
)
SELECT
    a.id,
    a.opened_date,
    as.avg_amount,
    as.shell_count
FROM accounts a
INNER JOIN account_summary as ON a.id = as.account_id
ORDER BY shell_count DESC, id ASC
LIMIT 5
