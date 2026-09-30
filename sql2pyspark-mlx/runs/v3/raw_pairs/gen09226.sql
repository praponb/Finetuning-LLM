SELECT customer_id, status, total_balance FROM (SELECT customer_id, status, SUM(balance) AS total_balance FROM accounts GROUP BY customer_id, status) AS t WHERE total_balance > 363000
