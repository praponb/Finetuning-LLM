SELECT s.id, s.status FROM shipments s WHERE s.carrier IS NULL AND s.order_id IN (SELECT id FROM orders WHERE channel IN ('store', 'mobile', 'partner'))
