SELECT c.city, MAX(o.amount) AS max_amount FROM orders o LEFT JOIN customers c ON o.customer_id = c.id JOIN stores s ON o.store_id = s.id GROUP BY c.city
