SELECT capacity AS qty
FROM warehouses
EXCEPT
SELECT id
FROM skus
WHERE sku_code IS NOT NULL
ORDER BY qty DESC
