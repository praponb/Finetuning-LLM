SELECT debut_year AS key_id, country AS category, 'artist' AS src
FROM artists
UNION ALL
SELECT artist_id, title, 'track' AS src
FROM tracks
WHERE duration_sec > 209
ORDER BY key_id, category
