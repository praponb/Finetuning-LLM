SELECT status, 'order' AS source
FROM orders
WHERE store_id IS NULL
UNION ALL
SELECT status, 'shipment' AS source
FROM shipments
ORDER BY source DESC;
