SELECT id, carrier
FROM shipments
WHERE YEAR(shipped_date) = 2023
UNION
SELECT id, carrier
FROM shipments
WHERE order_id IS NOT NULL
