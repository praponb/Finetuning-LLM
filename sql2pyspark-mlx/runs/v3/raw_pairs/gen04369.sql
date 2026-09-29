SELECT MONTH(opened_date) AS opened_month, MAX(balance) AS max_balance, ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
GROUP BY opened_month
