SELECT id
FROM skus
WHERE UPPER(TRIM(sku_code)) LIKE 'E%'
EXCEPT
SELECT id
FROM warehouses
WHERE opened_on IS NOT NULL
  AND capacity >= 4912
ORDER BY id DESC
