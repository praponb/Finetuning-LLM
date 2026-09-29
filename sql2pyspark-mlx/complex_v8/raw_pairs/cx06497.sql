SELECT 'artist' AS source, id, debut_year AS key_id
FROM artists
UNION
SELECT 'track' AS source, bpm AS id, artist_id AS key_id
FROM tracks
ORDER BY source
