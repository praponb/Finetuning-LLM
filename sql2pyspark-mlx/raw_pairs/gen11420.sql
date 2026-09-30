SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS cnt, SUM(SUM(amount)) OVER (ORDER BY customer_id) AS cumulative_amount FROM orders GROUP BY customer_id ORDER BY total_amount ASC
