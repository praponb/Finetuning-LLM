SELECT id
FROM skus
WHERE weight_kg BETWEEN 8.5 AND 18.5
  AND unit_cost > 158.7
INTERSECT
SELECT id
FROM warehouses
WHERE city <> 'Bangkok'
ORDER BY id
