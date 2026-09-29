SELECT o.customer_id, s.store_name, c.city, c.credit_limit FROM orders o LEFT JOIN stores s ON o.store_id = s.id LEFT JOIN customers c ON o.customer_id = c.id
