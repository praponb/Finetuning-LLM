WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        CASE WHEN source IN ('playlist', 'album') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE track_id <= 14
      AND device IS NOT NULL
)
SELECT ms_played, played_at AS event_time, listener_id AS qty
FROM scoped_plays
WHERE ms_played <= 230798
UNION ALL
SELECT id, played_at, track_id
FROM plays
WHERE HOUR(played_at) BETWEEN 10 AND 16
UNION ALL
SELECT track_id, played_at, listener_id
FROM plays
ORDER BY event_time DESC, ms_played
