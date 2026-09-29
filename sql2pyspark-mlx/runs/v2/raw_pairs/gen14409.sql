SELECT o.discount, o.channel, s.city, c.is_active FROM orders o INNER JOIN stores s ON o.store_id = s.id INNER JOIN customers c ON o.customer_id = c.id;
