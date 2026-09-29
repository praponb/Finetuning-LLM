SELECT s.id, s.carrier, o.channel, c.email FROM shipments s INNER JOIN orders o ON s.order_id = o.id LEFT JOIN customers c ON o.customer_id = c.id
