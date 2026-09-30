SELECT id AS key_id
FROM listeners
WHERE birth_year > 1970
INTERSECT
SELECT debut_year
FROM artists
WHERE debut_year BETWEEN 1996 AND 2015
  AND LOWER(artist_name) LIKE '%a %'
