SELECT order_id, COUNT(DISTINCT amount) AS unique_orders, MIN(paid_at) AS first_paid_at FROM payments WHERE amount > 970 GROUP BY order_id HAVING AVG(amount) < 1250;
