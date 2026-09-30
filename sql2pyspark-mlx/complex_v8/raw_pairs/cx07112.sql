SELECT
    track_id AS entity_id,
    played_at,
    listener_id,
    'play' AS origin
FROM plays
WHERE device <> 'car'
  AND HOUR(played_at) BETWEEN 8 AND 10
UNION ALL
SELECT
    ms_played,
    played_at,
    id,
    'play_2' AS origin
FROM plays
WHERE device <> 'phone'
UNION ALL
SELECT
    id,
    played_at,
    track_id,
    'play_3' AS origin
FROM plays
