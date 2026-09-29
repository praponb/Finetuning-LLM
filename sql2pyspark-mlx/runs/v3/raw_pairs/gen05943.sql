SELECT shipments.id, shipments.shipped_date, shipments.cost
FROM shipments
WHERE shipments.order_id IN (SELECT id FROM orders WHERE channel IS NULL)
ORDER BY id DESC
LIMIT 10
