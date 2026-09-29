SELECT amount, 'payment' source
FROM payments
UNION
SELECT cost amount, 'shipment' source
FROM shipments
ORDER BY amount
