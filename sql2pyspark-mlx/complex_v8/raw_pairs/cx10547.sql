SELECT artist_id AS entity_id
FROM tracks
WHERE duration_sec <= 309
EXCEPT
SELECT listener_id
FROM plays
WHERE MONTH(played_at) IN (1, 3)
  AND source NOT IN ('search', 'album')
