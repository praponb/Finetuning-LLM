SELECT shipments.id, shipments.shipped_date FROM shipments WHERE shipments.order_id NOT IN (SELECT id FROM orders WHERE customer_id = 11)
