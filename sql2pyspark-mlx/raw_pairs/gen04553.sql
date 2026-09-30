SELECT c.city, COUNT(o.amount) AS amount_count FROM orders o JOIN stores s ON s.id = o.store_id LEFT JOIN customers c ON c.id = o.customer_id GROUP BY c.city;
