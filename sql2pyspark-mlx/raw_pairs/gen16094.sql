SELECT MONTH(opened_date) opened_month, AVG(balance) avg_balance FROM accounts GROUP BY 1 ORDER BY avg_balance;
