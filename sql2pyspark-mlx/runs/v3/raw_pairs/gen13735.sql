SELECT id, order_id, delivered_date
FROM shipments
WHERE order_id IS NULL;
