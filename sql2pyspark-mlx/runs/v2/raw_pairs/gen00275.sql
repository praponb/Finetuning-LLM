SELECT order_id, LOWER(carrier) AS carrier_lower
FROM shipments
WHERE order_id IS NOT NULL
ORDER BY carrier_lower DESC, 1 DESC
