SELECT account_type, DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(balance) AS total_balance
FROM accounts
GROUP BY account_type, DATE_FORMAT(opened_date, 'yyyy-MM')
HAVING total_balance < 318000
ORDER BY opened_month DESC
