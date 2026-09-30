SELECT shipments.id, shipments.status, shipments.shipped_date FROM shipments WHERE shipments.order_id IN (SELECT id FROM orders WHERE amount <= 1050) ORDER BY status, id LIMIT 3;
