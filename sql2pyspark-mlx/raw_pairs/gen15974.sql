SELECT status, 'order' AS origin
FROM orders
UNION
SELECT status, 'shipment' AS origin
FROM shipments
ORDER BY origin DESC;
