SELECT o.id, o.shipping_fee, c.email, c.name FROM orders o LEFT JOIN customers c ON c.id = o.customer_id WHERE o.customer_id IN (21, 28) ORDER BY c.name, id LIMIT 1;
