SELECT o.id, o.channel, c.is_active
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
