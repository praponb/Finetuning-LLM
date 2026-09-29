SELECT debut_year
FROM artists
WHERE debut_year BETWEEN 1994 AND 2016
  AND country IN ('US', 'TH')
EXCEPT
SELECT id
FROM artists
