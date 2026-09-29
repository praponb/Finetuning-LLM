SELECT id, shipped_date
FROM shipments
WHERE shipped_date = (SELECT MIN(shipped_date) FROM shipments WHERE order_id IS NOT NULL);
