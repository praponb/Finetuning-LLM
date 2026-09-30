WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE NOT is_skipped
      AND played_at < TIMESTAMP '2025-02-24 11:00:00'
),
latest_per_key AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id DESC) AS rn
        FROM recent_plays
    ) ranked
    WHERE rn = 1
),
enriched AS (
    -- Add calculated columns
    SELECT
        *,
        TO_DATE(played_at) AS played_at_date,
        ABS(ms_played - 180879) AS ms_played_distance,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix
    FROM latest_per_key
    WHERE track_id IS NOT NULL
)
SELECT
    id,
    listener_id,
    track_id,
    device,
    is_skipped,
    source,
    played_at_date,
    ms_played_distance,
    device_prefix
FROM enriched
