SELECT s.id, s.status FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE store_id IN (13, 21, 23, 26));
