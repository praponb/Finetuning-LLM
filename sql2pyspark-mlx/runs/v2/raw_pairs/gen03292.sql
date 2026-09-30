SELECT s.order_id, o.channel
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id;
