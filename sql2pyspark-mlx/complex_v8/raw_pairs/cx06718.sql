SELECT id AS qty
FROM skus
WHERE weight_kg <= 14.6
INTERSECT
SELECT id
FROM skus
ORDER BY qty
