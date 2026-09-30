SELECT sku_code
FROM skus
INTERSECT ALL
SELECT wh_code AS sku_code
FROM warehouses
ORDER BY sku_code
