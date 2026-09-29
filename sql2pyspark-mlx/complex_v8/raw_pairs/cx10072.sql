SELECT id AS entity_id
FROM tracks
WHERE duration_sec >= 165
INTERSECT
SELECT ms_played
FROM plays
WHERE ms_played < 302279
  AND track_id >= 11
ORDER BY entity_id
