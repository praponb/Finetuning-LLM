SELECT id, cost, status
FROM shipments
WHERE cost >= (SELECT MAX(cost) FROM shipments WHERE status IN ('RETURNED', 'LOST'))
ORDER BY cost DESC;
