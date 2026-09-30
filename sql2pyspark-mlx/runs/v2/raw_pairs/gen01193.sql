SELECT s.id, s.order_id, s.cost, o.channel, o.customer_id, o.store_id FROM shipments s LEFT JOIN orders o ON s.order_id = o.id WHERE o.discount > 0.3 ORDER BY s.cost DESC, id LIMIT 5;
