SELECT status
FROM shipments
WHERE order_id IS NOT NULL
UNION ALL
SELECT status
FROM shipments
WHERE YEAR(delivered_date) >= 2024;
