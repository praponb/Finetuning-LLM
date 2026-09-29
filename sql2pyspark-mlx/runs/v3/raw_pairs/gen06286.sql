SELECT account_type, ROUND(AVG(balance), 2) AS avg_balance, MIN(opened_date) AS min_opened_date
FROM accounts
GROUP BY 1
ORDER BY min_opened_date;
