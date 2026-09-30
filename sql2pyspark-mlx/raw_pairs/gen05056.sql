SELECT cost AS value, 'shipment' AS record_type
FROM shipments
WHERE cost IS NOT NULL
UNION ALL
SELECT price AS value, 'product' AS record_type
FROM products
WHERE price IS NOT NULL
