SELECT s.id, s.shipped_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE store_id IN (4, 12, 35));
