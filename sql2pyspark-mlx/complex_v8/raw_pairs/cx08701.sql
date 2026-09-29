SELECT id AS entity_id, artist_name AS label
FROM artists
MINUS
SELECT track_id, source
FROM plays
WHERE listener_id >= 11
  AND HOUR(played_at) BETWEEN 8 AND 11
