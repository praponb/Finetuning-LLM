SELECT status, ROUND(AVG(balance), 2) AS avg_balance, MIN(opened_date) AS first_opened_date
FROM accounts
WHERE balance BETWEEN 52000 AND 72000
GROUP BY status
ORDER BY avg_balance DESC;
