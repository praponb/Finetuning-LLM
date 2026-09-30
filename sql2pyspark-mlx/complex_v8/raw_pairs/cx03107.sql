SELECT id AS ref_id, sku_code AS name, 'sku' AS src
FROM skus
UNION
SELECT id, sku_code, 'sku_2' AS src
FROM skus
WHERE unit_cost > 134.3
