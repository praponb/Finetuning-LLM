SELECT sku_code
FROM skus
WHERE is_active = FALSE
  AND UPPER(TRIM(sku_code)) LIKE 'A%'
EXCEPT
SELECT category AS sku_code
FROM skus
ORDER BY sku_code
