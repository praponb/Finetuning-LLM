SELECT id, shipped_date, status
FROM shipments
WHERE order_id IS NULL
ORDER BY id DESC
