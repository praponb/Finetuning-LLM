SELECT debut_year
FROM artists
MINUS
SELECT debut_year
FROM artists
WHERE is_verified = TRUE
  AND genre NOT IN ('Pop', 'Jazz')
ORDER BY debut_year DESC
