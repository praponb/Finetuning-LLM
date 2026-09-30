SELECT account_type, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, AVG(balance) AS avg_balance
FROM accounts
GROUP BY account_type, opened_month
ORDER BY avg_balance, account_type, opened_month
LIMIT 5;
