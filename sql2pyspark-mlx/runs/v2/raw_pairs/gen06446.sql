SELECT s.id, s.status, s.delivered_date FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE channel = 'store') ORDER BY delivered_date DESC;
