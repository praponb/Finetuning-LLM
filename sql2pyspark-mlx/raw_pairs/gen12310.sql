SELECT o.id, o.order_date, o.status
FROM orders o
WHERE EXISTS (SELECT 1 FROM shipments s WHERE s.order_id = o.id AND s.delivered_date >= '2024-04-01')
