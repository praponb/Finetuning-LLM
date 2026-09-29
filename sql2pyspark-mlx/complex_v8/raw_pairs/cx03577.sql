SELECT id AS key_id, username
FROM listeners
WHERE username IS NOT NULL
  AND country = 'JP'
INTERSECT
SELECT debut_year AS key_id, genre AS username
FROM artists
WHERE genre = 'Classical'
ORDER BY key_id
