SELECT id
FROM listeners
WHERE birth_year >= 1967
  AND country IS NOT NULL
EXCEPT
SELECT debut_year
FROM artists
ORDER BY id DESC
