SELECT s.id, s.delivered_date
FROM shipments s
WHERE s.order_id NOT IN (SELECT id FROM orders WHERE order_date <= '2025-04-01')
ORDER BY delivered_date ASC, id DESC
LIMIT 10
