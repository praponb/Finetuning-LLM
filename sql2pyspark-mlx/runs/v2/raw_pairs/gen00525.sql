SELECT id, carrier, status
FROM shipments
WHERE order_id = 1
ORDER BY carrier DESC;
