SELECT id, order_id
FROM shipments
WHERE status = 'IN_TRANSIT'
UNION ALL
SELECT id, order_id
FROM shipments
WHERE status IN ('LOST', 'DELIVERED')
ORDER BY order_id
