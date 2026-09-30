SELECT 'shipment' AS record_type, cost AS value
FROM shipments
UNION ALL
SELECT 'store_sale' AS record_type, revenue AS value
FROM store_sales
WHERE store_id = 1;
