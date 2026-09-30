SELECT o.id, o.channel, s.city, c.is_active, c.age FROM orders o LEFT JOIN stores s ON o.store_id = s.id JOIN customers c ON o.customer_id = c.id WHERE s.sqft = 8080
