SELECT DATE_FORMAT(opened_date, 'yyyy-MM') AS opened_month, account_type, ROUND(AVG(balance), 2) AS avg_balance FROM accounts WHERE status IS NOT NULL GROUP BY 1, 2 ORDER BY avg_balance;
