SELECT order_id
FROM shipments
WHERE carrier NOT IN ('Flash', 'DHL', 'UPS')
ORDER BY order_id DESC
