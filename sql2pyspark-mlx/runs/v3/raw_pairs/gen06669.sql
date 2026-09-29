SELECT id, shipped_date, status
FROM shipments
WHERE shipped_date > (SELECT MAX(shipped_date) FROM shipments WHERE carrier IS NOT NULL)
ORDER BY shipped_date, id
LIMIT 1
