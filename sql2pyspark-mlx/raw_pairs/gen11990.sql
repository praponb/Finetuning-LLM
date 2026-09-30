SELECT 'product' AS record_type, launch_date
FROM products
WHERE product_name LIKE 'U%'
UNION
SELECT 'shipment' AS record_type, delivered_date
FROM shipments
WHERE order_id IN (17, 23, 33, 39);
