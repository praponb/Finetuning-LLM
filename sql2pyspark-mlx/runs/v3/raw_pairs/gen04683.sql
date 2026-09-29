WITH AccountTotals AS (
    SELECT account_id, SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) AS shell_count, ROUND(AVG(amount), 2) AS avg_amount
    FROM transactions
    GROUP BY account_id
)
SELECT
    a.id,
    a.balance,
    A.shell_count,
    A.avg_amount
FROM accounts a
INNER JOIN AccountTotals A ON a.id = A.account_id
WHERE A.shell_count >= 43;
