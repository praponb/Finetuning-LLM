SELECT birth_year AS key_id
FROM listeners
INTERSECT
SELECT id
FROM artists
WHERE debut_year <= 2020
  AND country = 'BR'
