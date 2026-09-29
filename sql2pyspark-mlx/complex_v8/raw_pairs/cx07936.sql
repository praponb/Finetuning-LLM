SELECT id AS entity_id
FROM artists
WHERE UPPER(TRIM(artist_name)) LIKE 'P%'
  AND debut_year > 1994
INTERSECT
SELECT artist_id AS entity_id
FROM tracks
