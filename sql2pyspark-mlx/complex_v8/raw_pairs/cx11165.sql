WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix,
        ABS(ms_played - 168782) AS ms_played_distance
    FROM plays
    WHERE listener_id IS NOT NULL
      AND track_id IS NOT NULL
),
screened AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM recent s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.source IS NOT NULL
        )
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM screened
    WHERE ms_played_distance > (SELECT MIN(ms_played_distance) FROM screened)
),
scored AS (
    -- Add calculated columns
    SELECT
        *,
        FLOOR(ms_played_distance / 50000) * 50000 AS ms_played_distance_bucket,
        COALESCE(ms_played_distance, 0) AS ms_played_distance_filled,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM filtered
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count_2,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count_3
    FROM scored s
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at,
        (SELECT COUNT(CASE WHEN p.source NOT IN ('search', 'playlist') THEN 1 END) FROM plays p WHERE p.listener_id = s.listener_id) AS play_source_count
    FROM with_stats s
)
SELECT *
FROM (
    SELECT first_played_at, device_or_unknown, ms_played_distance
    FROM with_counts
) src
PIVOT (
    MIN(ms_played_distance)
    FOR device_or_unknown IN ('tv', 'phone')
)
