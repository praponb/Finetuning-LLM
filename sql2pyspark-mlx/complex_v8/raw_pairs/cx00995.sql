WITH prepared AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE is_skipped = TRUE
)
SELECT
    track_id AS qty,
    listener_id AS ref_id,
    played_at AS event_time,
    'play' AS src
FROM prepared
UNION
SELECT
    id,
    listener_id,
    played_at,
    'play_2' AS src
FROM plays
WHERE track_id <= 2
  AND ms_played <= 220498
ORDER BY src, ref_id
