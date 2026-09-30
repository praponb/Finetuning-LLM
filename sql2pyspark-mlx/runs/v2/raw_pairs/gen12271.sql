SELECT status, MIN(opened_date) AS min_opened_date, ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
GROUP BY 1
HAVING avg_balance < 21
ORDER BY avg_balance, status;
