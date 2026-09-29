SELECT customer_id, SUM(discount) AS total_discount, COUNT(*) AS total_count FROM orders GROUP BY customer_id HAVING MIN(amount) >= 1200 ORDER BY total_discount DESC, customer_id LIMIT 5;
