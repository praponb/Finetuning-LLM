SELECT s.id, s.delivered_date, s.status, s.carrier FROM shipments s WHERE s.cost > 74 AND s.order_id IN (SELECT id FROM orders WHERE channel IN ('store', 'web', 'partner'))
