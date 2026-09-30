SELECT orders.id, orders.customer_id FROM orders WHERE NOT EXISTS (SELECT 1 FROM shipments WHERE shipments.order_id = orders.id);
