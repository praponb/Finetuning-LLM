SELECT
    'sku' AS src,
    id,
    unit_cost,
    category
FROM skus
WHERE SIZE(labels) >= 1
UNION ALL
SELECT
    'sku_2' AS src,
    id,
    unit_cost,
    sku_code
FROM skus
WHERE ARRAY_CONTAINS(labels, 'express')
ORDER BY unit_cost, id
