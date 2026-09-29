SELECT source
FROM plays
INTERSECT
SELECT title AS source
FROM tracks
WHERE artist_id IS NOT NULL
