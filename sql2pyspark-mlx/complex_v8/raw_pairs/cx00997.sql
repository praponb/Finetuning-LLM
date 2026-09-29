SELECT
    id,
    source,
    played_at,
    'play' AS origin
FROM plays
UNION
SELECT
    track_id AS id,
    device AS source,
    played_at,
    'play_2' AS origin
FROM plays
WHERE device IN ('car', 'desktop', 'phone')
  AND track_id >= 21
