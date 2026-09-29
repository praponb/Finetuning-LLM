SELECT cost, 'shipment' AS source
FROM shipments
UNION ALL
SELECT amount AS cost, 'payment' AS source
FROM payments;
