WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix
    FROM plays
    WHERE ms_played > 95960
),
unioned AS (
    -- Stack the branches
    SELECT source AS name, device AS name_2, played_at
    FROM base
    WHERE listener_id IS NOT NULL
    UNION
    SELECT source, device, played_at
    FROM plays
)
SELECT name, COUNT(*) AS n_rows, MAX(played_at) AS latest_played_at
FROM unioned
GROUP BY name
ORDER BY name
