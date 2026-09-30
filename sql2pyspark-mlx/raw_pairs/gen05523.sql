SELECT status, avg_balance
FROM (
    SELECT status, ROUND(AVG(balance), 2) AS avg_balance
    FROM accounts
    GROUP BY status
) AS sub
WHERE avg_balance < 36
ORDER BY avg_balance DESC;
