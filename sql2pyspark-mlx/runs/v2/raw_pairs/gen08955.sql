SELECT s.city, AVG(o.amount) AS avg_amount FROM orders o INNER JOIN stores s ON o.store_id = s.id INNER JOIN customers c ON o.customer_id = c.id GROUP BY s.city ORDER BY city LIMIT 1;
