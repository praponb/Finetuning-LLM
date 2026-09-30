SELECT status, MONTH(opened_date) AS opened_month, ROUND(AVG(balance), 2) AS avg_balance
FROM accounts
GROUP BY status, opened_month
ORDER BY opened_month DESC;
