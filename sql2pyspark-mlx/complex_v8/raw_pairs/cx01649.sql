SELECT id AS ref_id, city AS label
FROM warehouses
INTERSECT ALL
SELECT id, sku_code
FROM skus
ORDER BY ref_id DESC
