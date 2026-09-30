SELECT
    track_id,
    played_at,
    listener_id AS qty,
    'play' AS origin
FROM plays
WHERE ms_played > 107912
UNION
SELECT
    listener_id AS track_id,
    played_at,
    ms_played AS qty,
    'play_2' AS origin
FROM plays
ORDER BY track_id DESC, played_at
