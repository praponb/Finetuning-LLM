SELECT shipments.id, shipments.shipped_date, shipments.order_id, shipments.cost FROM shipments WHERE shipments.order_id IN (SELECT id FROM orders WHERE store_id IS NULL);
