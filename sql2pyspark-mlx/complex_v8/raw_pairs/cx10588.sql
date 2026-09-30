SELECT device AS category
FROM plays
INTERSECT
SELECT title
FROM tracks
WHERE UPPER(TRIM(title)) LIKE 'S%'
  AND duration_sec < 357
