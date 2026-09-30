SELECT id, shipped_date
FROM shipments
WHERE shipped_date = (SELECT MIN(shipped_date) FROM shipments)
ORDER BY id DESC, shipped_date;
