SELECT s.id, s.order_id, s.cost FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE store_id = 6);
