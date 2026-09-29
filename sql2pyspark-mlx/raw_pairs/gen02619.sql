SELECT s.id, s.order_id FROM shipments s WHERE s.order_id NOT IN (SELECT id FROM orders WHERE customer_id IN (16, 17, 35, 40)) ORDER BY id, s.order_id;
