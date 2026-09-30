SELECT capacity, wh_code
FROM warehouses
WHERE LENGTH(TRIM(wh_code)) >= 4
  AND city NOT IN ('Bangkok', 'Kuala Lumpur')
UNION ALL
SELECT id, category
FROM skus
WHERE category = 'Beauty'
  AND ARRAY_CONTAINS(labels, 'express')
