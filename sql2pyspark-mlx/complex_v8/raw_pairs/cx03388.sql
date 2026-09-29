SELECT id AS qty, sku_code AS category
FROM skus
EXCEPT
SELECT capacity AS qty, wh_code AS category
FROM warehouses
ORDER BY qty
