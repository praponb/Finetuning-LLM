SELECT o.id, o.customer_id, c.email, s.region FROM orders o INNER JOIN customers c ON o.customer_id = c.id FULL OUTER JOIN stores s ON o.store_id = s.id WHERE o.amount <= 560 ORDER BY customer_id
