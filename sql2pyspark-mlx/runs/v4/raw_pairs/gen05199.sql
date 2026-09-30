SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, SUM(CASE WHEN status = 'CLOSED' THEN balance ELSE 0 END) AS closed_balance, SUM(balance) AS total_balance
FROM accounts
GROUP BY DATE_FORMAT(opened_date, 'yyyy-MM')
ORDER BY total_balance DESC;
