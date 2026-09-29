SELECT country AS label
FROM artists
INTERSECT
SELECT artist_name AS label
FROM artists
WHERE LENGTH(TRIM(artist_name)) >= 4
ORDER BY label DESC
