SELECT customer_id, SUM(amount) AS total_amount, COUNT(*) AS cnt, RANK() OVER (ORDER BY SUM(amount) DESC) AS rnk FROM orders GROUP BY customer_id;
