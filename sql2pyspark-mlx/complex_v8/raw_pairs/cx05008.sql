SELECT genre
FROM artists
WHERE genre IN ('Classical', 'Jazz')
  AND debut_year <= 2006
INTERSECT
SELECT device AS genre
FROM plays
ORDER BY genre
