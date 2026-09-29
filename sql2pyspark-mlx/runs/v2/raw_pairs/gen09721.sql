SELECT MONTH(opened_date) AS opened_month, AVG(balance) AS avg_balance, MIN(balance) AS min_balance FROM accounts GROUP BY opened_month ORDER BY opened_month DESC, 3 DESC;
