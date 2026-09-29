SELECT s.id, s.delivered_date, s.cost, s.status FROM shipments s WHERE s.order_id IN (SELECT id FROM orders WHERE channel IS NULL);
