SELECT id, shipped_date
FROM shipments
WHERE shipped_date = (SELECT MAX(shipped_date) FROM shipments);
