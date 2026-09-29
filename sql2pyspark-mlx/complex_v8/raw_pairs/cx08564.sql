SELECT debut_year, country
FROM artists
EXCEPT
SELECT id, username
FROM listeners
WHERE country NOT IN ('KR', 'JP')
  AND SIZE(interests) >= 1
ORDER BY country DESC
