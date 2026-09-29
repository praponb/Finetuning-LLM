SELECT s.order_id, o.amount, o.status
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
WHERE s.order_id IN (19, 32)
