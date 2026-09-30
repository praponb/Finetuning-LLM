SELECT artist_name
FROM artists
WHERE debut_year > 1991
  AND genre = 'Indie'
INTERSECT
SELECT source
FROM plays
ORDER BY artist_name DESC
