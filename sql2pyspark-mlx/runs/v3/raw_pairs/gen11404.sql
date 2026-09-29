SELECT carrier, order_id
FROM shipments
WHERE order_id IS NULL AND order_id IN (5, 11, 20, 38)
ORDER BY order_id DESC
