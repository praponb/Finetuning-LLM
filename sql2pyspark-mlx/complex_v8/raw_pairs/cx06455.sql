SELECT id AS ref_id
FROM skus
EXCEPT
SELECT id
FROM skus
WHERE unit_cost > 134.3
  AND NOT ARRAY_CONTAINS(labels, 'fragile')
