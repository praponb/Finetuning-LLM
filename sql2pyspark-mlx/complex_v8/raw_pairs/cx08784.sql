WITH base_plays AS (
    -- Filter plays
    SELECT *
    FROM plays
    WHERE played_at >= TIMESTAMP '2025-01-25 10:00:00'
)
SELECT track_id AS qty, played_at AS event_time
FROM base_plays
UNION
SELECT track_id, played_at
FROM plays
WHERE track_id IS NOT NULL
