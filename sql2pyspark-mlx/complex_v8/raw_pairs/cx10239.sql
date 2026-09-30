SELECT id
FROM tracks
WHERE UPPER(TRIM(title)) LIKE 'P%'
  AND duration_sec < 357
INTERSECT
SELECT birth_year AS id
FROM listeners
ORDER BY id DESC
