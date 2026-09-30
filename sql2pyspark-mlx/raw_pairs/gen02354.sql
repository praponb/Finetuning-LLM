SELECT orders.id, orders.store_id, orders.discount FROM orders WHERE orders.id IN (SELECT order_id FROM shipments);
