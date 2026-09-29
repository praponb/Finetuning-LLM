WITH base AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        ABS(ms_played - 245815) AS ms_played_distance
    FROM plays
),
latest AS (
    -- Keep the latest row per listener_id
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        ms_played_distance
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY listener_id ORDER BY ms_played DESC, id DESC) AS rn
        FROM base
    ) ranked
    WHERE rn = 1
)
SELECT
    device,
    is_skipped,
    ROUND(AVG(ms_played), 2) AS avg_ms_played,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(ms_played_distance) FILTER (WHERE is_skipped = TRUE) AS ms_played_distance_is_skipped_true
FROM latest
GROUP BY GROUPING SETS ((device, is_skipped), (device), (is_skipped), ())
