SELECT id
FROM artists
INTERSECT ALL
SELECT debut_year
FROM artists
WHERE genre IS NOT NULL
  AND country IN ('BR', 'TH')
ORDER BY id DESC
