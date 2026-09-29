SELECT s.id, s.order_id FROM shipments s WHERE s.order_id IS NOT NULL AND s.order_id IN (SELECT id FROM orders WHERE channel = 'store') ORDER BY order_id
