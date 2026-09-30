WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device
    FROM plays
    WHERE device NOT IN ('phone', 'speaker')
      AND ms_played > 73625
)
SELECT source, device AS name, played_at
FROM plays
UNION
SELECT device, source, played_at
FROM plays
