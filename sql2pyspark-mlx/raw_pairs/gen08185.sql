SELECT o.id, o.store_id, c.is_active, c.name FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.channel != 'web' ORDER BY name, o.store_id DESC;
