SELECT amount AS value, 'order' AS source
FROM orders
UNION ALL
SELECT cost AS value, 'shipment' AS source
FROM shipments
