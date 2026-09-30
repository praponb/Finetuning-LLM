SELECT s.id, s.delivered_date, s.status
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE order_date < '2025-08-01')
ORDER BY s.status
