SELECT 'product' AS record_type, price
FROM products
UNION ALL
SELECT 'shipment' AS record_type, cost
FROM shipments;
