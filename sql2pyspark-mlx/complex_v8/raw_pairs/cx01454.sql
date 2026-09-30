SELECT id
FROM skus
WHERE unit_cost < 306.4
  AND ARRAY_CONTAINS(labels, 'fragile')
INTERSECT ALL
SELECT id
FROM warehouses
ORDER BY id
