WITH filtered AS (
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
        SUBSTRING(TRIM(source), 1, 3) AS source_prefix,
        ABS(ms_played - 130888) AS ms_played_distance
    FROM plays
),
play_stats AS (
    -- Per-listener plays metrics
    SELECT listener_id, MIN(played_at) AS first_played_at
    FROM plays
    GROUP BY listener_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.first_played_at
    FROM filtered s
    LEFT JOIN play_stats p ON p.listener_id = s.listener_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        ms_played,
        device,
        source,
        source_prefix,
        ms_played_distance,
        first_played_at,
        LAG(played_at, 2) OVER (PARTITION BY track_id ORDER BY ms_played DESC, id) AS prev2_played_at,
        NTH_VALUE(ms_played_distance, 2) OVER (PARTITION BY is_skipped, track_id ORDER BY first_played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_distance,
        ROUND(AVG(ms_played) OVER (ORDER BY ms_played_distance, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        COUNT(*) OVER (PARTITION BY TO_DATE(first_played_at), is_skipped) AS rows_in_first_played_at_to_date_is_skipped
    FROM with_counts
),
latest_per_key AS (
    -- Keep the earliest row per device
    SELECT
        id,
        listener_id,
        ms_played,
        device,
        source,
        source_prefix,
        ms_played_distance,
        first_played_at,
        prev2_played_at,
        second_ms_played_distance,
        ms_played_avg_2,
        rows_in_first_played_at_to_date_is_skipped
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device ORDER BY first_played_at, id) AS rn
        FROM with_windows
    ) ranked
    WHERE rn = 1
),
play_stats_2 AS (
    -- Per-listener plays metrics
    SELECT
        listener_id,
        COUNT(CASE WHEN source NOT IN ('album', 'radio') THEN 1 END) AS play_source_count,
        MAX(ms_played) AS max_play_ms_played,
        COUNT(*) AS play_count
    FROM plays
    GROUP BY listener_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count,
        p.max_play_ms_played,
        COALESCE(p.play_count, 0) AS play_count
    FROM latest_per_key s
    LEFT JOIN play_stats_2 p ON p.listener_id = s.listener_id
),
qualified AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM with_stats s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id IS NOT NULL
              AND p.played_at >= TIMESTAMP '2025-01-19 14:00:00'
        )
)
-- Final projection
SELECT
    id,
    listener_id,
    ms_played,
    device,
    source,
    source_prefix,
    ms_played_distance,
    first_played_at,
    prev2_played_at,
    second_ms_played_distance,
    ms_played_avg_2,
    rows_in_first_played_at_to_date_is_skipped,
    play_source_count,
    play_count,
    LENGTH(TRIM(device)) AS device_len
FROM qualified
WHERE source_prefix IS NOT NULL
ORDER BY device DESC
