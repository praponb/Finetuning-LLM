SELECT delivered_date, 'shipment' AS source
FROM shipments
UNION
SELECT launch_date AS delivered_date, 'product' AS source
FROM products
WHERE supplier_id IN (4, 5, 18, 37);
