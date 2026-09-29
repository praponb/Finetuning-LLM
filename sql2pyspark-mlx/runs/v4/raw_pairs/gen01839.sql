SELECT id, delivered_date, carrier
FROM shipments
WHERE order_id IN (18, 22, 30)
