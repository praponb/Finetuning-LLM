SELECT amount, 'order' source
FROM orders
UNION ALL
SELECT cost, 'shipment' source
FROM shipments
