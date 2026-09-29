SELECT
    listener_id AS ref_id,
    device AS name,
    played_at AS happened_at,
    'play' AS origin
FROM plays
WHERE listener_id <= 19
UNION ALL
SELECT
    track_id AS ref_id,
    device AS name,
    played_at AS happened_at,
    'play_2' AS origin
FROM plays
ORDER BY origin, name
