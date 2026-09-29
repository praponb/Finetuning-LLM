SELECT id
FROM skus
WHERE unit_cost <= 251.2
  AND sku_code IS NOT NULL
EXCEPT
SELECT id
FROM skus
ORDER BY id
