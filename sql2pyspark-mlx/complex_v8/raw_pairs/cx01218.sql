SELECT id, category, 'sku' AS origin
FROM skus
UNION ALL
SELECT id, wh_code, 'warehouse' AS origin
FROM warehouses
WHERE LOWER(wh_code) LIKE '%kk%'
UNION ALL
SELECT id, sku_code, 'sku_3' AS origin
FROM skus
WHERE SIZE(labels) >= 1
ORDER BY category DESC, id
