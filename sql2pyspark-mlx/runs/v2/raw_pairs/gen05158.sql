SELECT s.id, s.carrier FROM shipments s WHERE s.shipped_date < '2025-01-01' AND s.order_id IN (SELECT id FROM orders WHERE discount > 0.39)
