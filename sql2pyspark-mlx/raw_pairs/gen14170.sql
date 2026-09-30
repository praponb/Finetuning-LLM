SELECT MONTH(opened_date) AS opened_month, COUNT(DISTINCT balance) AS unique_customers, SUM(balance) AS total_balance FROM accounts GROUP BY opened_month
