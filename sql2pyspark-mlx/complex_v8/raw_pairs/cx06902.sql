SELECT sku_code AS name, category
FROM skus
WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
UNION ALL
SELECT sku_code AS name, category
FROM skus
WHERE is_active = TRUE
  AND category = 'Toys'
