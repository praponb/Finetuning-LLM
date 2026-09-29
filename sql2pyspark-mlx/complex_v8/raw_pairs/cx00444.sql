SELECT username AS name
FROM listeners
WHERE signup_date >= DATE '2024-10-17'
EXCEPT
SELECT country
FROM artists
WHERE debut_year < 2016
  AND genre IS NOT NULL
ORDER BY name
