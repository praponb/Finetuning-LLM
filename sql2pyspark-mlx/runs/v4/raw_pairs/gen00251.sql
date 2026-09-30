SELECT shipments.id, shipments.carrier, shipments.status, shipments.order_id FROM shipments WHERE shipments.order_id NOT IN (SELECT id FROM orders WHERE store_id IN (1, 18, 30, 40)) ORDER BY status;
