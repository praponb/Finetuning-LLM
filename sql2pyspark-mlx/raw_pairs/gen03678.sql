SELECT status, avg_balance
FROM (
    SELECT status, ROUND(AVG(balance), 2) AS avg_balance
    FROM accounts
    GROUP BY status
) AS t
WHERE avg_balance >= 44
ORDER BY 1
LIMIT 10
