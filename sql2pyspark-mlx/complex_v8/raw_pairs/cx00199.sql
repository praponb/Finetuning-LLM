SELECT
    'play' AS origin,
    ms_played AS key_id,
    played_at,
    device AS category
FROM plays
UNION
SELECT
    'play_2' AS origin,
    ms_played,
    played_at,
    source
FROM plays
