SELECT id AS key_id
FROM listeners
WHERE LENGTH(TRIM(username)) >= 8
EXCEPT
SELECT id AS key_id
FROM artists
WHERE debut_year >= 1997
  AND country IS NOT NULL
