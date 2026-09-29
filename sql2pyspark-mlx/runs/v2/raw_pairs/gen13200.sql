SELECT id, shipped_date
FROM shipments
WHERE order_id IS NOT NULL;
