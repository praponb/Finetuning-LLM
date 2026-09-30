WITH ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY status ORDER BY delivered_date DESC) AS rn
    FROM shipments
    WHERE order_id IN (22, 26)
)
SELECT id, status, delivered_date
FROM ranked
WHERE rn = 1
