SELECT id, order_id
FROM shipments
WHERE shipped_date BETWEEN '2023-08-01' AND '2025-04-01'
UNION
SELECT id, order_id
FROM shipments
WHERE status IN ('IN_TRANSIT', 'DELIVERED')
