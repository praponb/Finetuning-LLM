SELECT status, SUM(balance) AS total_balance, ROUND(AVG(balance), 2) AS avg_balance, COUNT(DISTINCT balance) AS unique_customers FROM accounts GROUP BY status HAVING ROUND(AVG(balance), 2) >= 73;
