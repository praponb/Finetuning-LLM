SELECT 'sku' AS origin, id, weight_kg AS value
FROM skus
UNION ALL
SELECT 'sku_2' AS origin, id, weight_kg AS value
FROM skus
WHERE weight_kg >= 6.5
  AND LOWER(sku_code) LIKE '%l-%'
UNION ALL
SELECT 'sku_3' AS origin, id, unit_cost AS value
FROM skus
WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
  AND category = 'Apparel'
ORDER BY id, value
