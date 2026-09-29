SELECT status, 'order' AS source
FROM orders
WHERE customer_id = 25
UNION ALL
SELECT status, 'shipment' AS source
FROM shipments
WHERE carrier = 'DHL';
