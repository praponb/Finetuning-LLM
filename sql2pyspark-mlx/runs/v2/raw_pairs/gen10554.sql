SELECT orders.id, orders.channel, orders.status FROM orders WHERE NOT EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id) ORDER BY orders.channel;
