SELECT
    'play' AS source,
    device,
    played_at,
    listener_id AS key_id
FROM plays
WHERE source IS NOT NULL
UNION
SELECT
    'play_2' AS source,
    device,
    played_at,
    ms_played AS key_id
FROM plays
