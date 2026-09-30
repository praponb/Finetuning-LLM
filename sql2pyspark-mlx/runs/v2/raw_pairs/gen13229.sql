SELECT o.id, o.channel, o.store_id, c.is_active, c.name, s.sqft, s.opened_date FROM orders o LEFT OUTER JOIN customers c ON o.customer_id = c.id INNER JOIN stores s ON o.store_id = s.id;
