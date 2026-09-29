SELECT category
FROM skus
WHERE UPPER(TRIM(sku_code)) LIKE 'A%'
EXCEPT
SELECT category
FROM skus
WHERE ARRAY_CONTAINS(labels, 'express')
