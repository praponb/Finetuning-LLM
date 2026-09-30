SELECT status, 'payment' AS source
FROM payments
WHERE order_id IS NULL
UNION
SELECT status, 'shipment' AS source
FROM shipments;
