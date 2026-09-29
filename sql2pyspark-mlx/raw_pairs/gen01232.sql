SELECT o.id, o.status, s.region, c.age, c.is_active FROM orders o INNER JOIN stores s ON o.store_id = s.id LEFT JOIN customers c ON c.id = o.customer_id ORDER BY id DESC, c.age DESC
