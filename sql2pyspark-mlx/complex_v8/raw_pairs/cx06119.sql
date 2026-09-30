SELECT id, track_id AS key_id
FROM plays
WHERE is_skipped = FALSE
  AND device = 'car'
UNION ALL
SELECT bpm AS id, duration_sec AS key_id
FROM tracks
WHERE NOT is_explicit
  AND ARRAY_CONTAINS(moods, 'chill')
ORDER BY id, key_id
