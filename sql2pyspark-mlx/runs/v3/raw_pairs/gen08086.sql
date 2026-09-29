SELECT cost, 'shipment' AS source
FROM shipments
WHERE status <> 'DELIVERED'
UNION ALL
SELECT amount, 'order' AS source
FROM orders
ORDER BY source DESC;
