SELECT id, carrier, LENGTH(status) AS status_length
FROM shipments
WHERE cost IS NULL AND order_id = 18
