SELECT
    id,
    carrier,
    order_id,
    cost,
    CASE WHEN status = 'LOST' THEN 1 ELSE 0 END AS is_lost
FROM shipments
WHERE shipped_date >= '2023-10-01' AND order_id IS NOT NULL AND delivered_date BETWEEN '2024-05-31' AND '2025-02-01'
ORDER BY carrier ASC
