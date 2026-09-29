SELECT username
FROM listeners
WHERE ARRAY_CONTAINS(interests, 'live')
  AND plan NOT IN ('premium', 'student')
EXCEPT
SELECT title
FROM tracks
WHERE release_date IS NOT NULL
ORDER BY username
