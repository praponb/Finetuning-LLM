SELECT o.id, o.customer_id, o.store_id, o.status FROM orders o WHERE o.id IN (SELECT order_id FROM shipments)
