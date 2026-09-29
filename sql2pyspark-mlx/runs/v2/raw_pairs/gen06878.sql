SELECT carrier, shipped_date
FROM shipments
WHERE YEAR(shipped_date) = 2025
UNION ALL
SELECT carrier, shipped_date
FROM shipments
WHERE order_id IN (7, 19, 38, 39)
ORDER BY carrier
