SELECT id AS qty
FROM skus
INTERSECT
SELECT id
FROM skus
WHERE weight_kg >= 6.5
  AND is_active = FALSE
ORDER BY qty
