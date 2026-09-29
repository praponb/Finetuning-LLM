SELECT 'play' AS record_type, id, played_at
FROM plays
WHERE track_id IS NOT NULL
UNION
SELECT 'play_2' AS record_type, track_id AS id, played_at
FROM plays
ORDER BY record_type
