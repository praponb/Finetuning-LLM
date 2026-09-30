SELECT id AS entity_id
FROM artists
INTERSECT
SELECT debut_year
FROM artists
WHERE country IS NOT NULL
  AND UPPER(TRIM(artist_name)) LIKE 'N%'
ORDER BY entity_id
