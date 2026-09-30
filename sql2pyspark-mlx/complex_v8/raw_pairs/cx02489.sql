SELECT id AS ref_id
FROM skus
INTERSECT
SELECT id AS ref_id
FROM skus
WHERE category IS NOT NULL
  AND weight_kg < 18.5
