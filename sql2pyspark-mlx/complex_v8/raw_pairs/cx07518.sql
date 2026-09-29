SELECT 'warehouse' AS src, id AS key_id, city
FROM warehouses
UNION ALL
SELECT 'sku' AS src, id, sku_code
FROM skus
WHERE is_active = FALSE
ORDER BY city
