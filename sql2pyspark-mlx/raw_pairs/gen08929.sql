SELECT s.id, s.shipped_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE order_date > '2023-05-01') ORDER BY s.id LIMIT 50;
