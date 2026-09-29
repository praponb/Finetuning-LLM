SELECT id, shipped_date
FROM shipments
WHERE MONTH(shipped_date) IN (6, 9, 10)
UNION ALL
SELECT id, shipped_date
FROM shipments
WHERE delivered_date < '2025-06-01';
