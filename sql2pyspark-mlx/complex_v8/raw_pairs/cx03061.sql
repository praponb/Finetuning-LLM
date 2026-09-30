SELECT
    id,
    track_id,
    played_at AS event_time,
    'play' AS source
FROM plays
UNION ALL
SELECT
    id,
    ms_played AS track_id,
    played_at AS event_time,
    'play_2' AS source
FROM plays
WHERE NOT is_skipped
UNION ALL
SELECT
    track_id AS id,
    ms_played AS track_id,
    played_at AS event_time,
    'play_3' AS source
FROM plays
ORDER BY id
