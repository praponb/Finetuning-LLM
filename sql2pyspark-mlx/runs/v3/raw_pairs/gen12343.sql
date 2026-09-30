SELECT shipped_date, order_id
FROM shipments
WHERE delivered_date BETWEEN '2025-05-01' AND '2025-08-01'
UNION ALL
SELECT shipped_date, order_id
FROM shipments
WHERE status != 'DELIVERED';
