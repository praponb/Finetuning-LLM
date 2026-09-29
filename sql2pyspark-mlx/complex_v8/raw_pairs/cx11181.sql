WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ABS(ms_played - 195566) AS ms_played_distance
    FROM plays
    WHERE track_id <= 14
      AND NOT is_skipped
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM cleaned_plays s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.source IN ('playlist', 'album')
              AND p.track_id <= 17
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ms_played - LAG(ms_played) OVER w_device_by_ms_played_distance AS ms_played_change,
        SIZE(COLLECT_SET(listener_id) OVER w_source_device) AS distinct_listener_id_in_source_device,
        FIRST_VALUE(ms_played) OVER (PARTITION BY device ORDER BY ms_played_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        ROUND(ms_played_distance - AVG(ms_played_distance) OVER (PARTITION BY source, device), 2) AS ms_played_distance_vs_source_device_avg
    FROM with_activity
    WINDOW w_device_by_ms_played_distance AS (PARTITION BY device ORDER BY ms_played_distance, id), w_source_device AS (PARTITION BY source, device)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (PARTITION BY listener_id, source ORDER BY distinct_listener_id_in_source_device ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        NTH_VALUE(first_ms_played, 2) OVER (ORDER BY ms_played_change, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_first_ms_played,
        ROUND(AVG(ms_played_change) OVER w_distinct_listener_id_in_sour, 2) AS distinct_listener_id_in_source_device_avg_ms_pla,
        ROUND(ms_played_distance / NULLIF(SUM(ms_played_distance) OVER (PARTITION BY distinct_listener_id_in_source_device), 0), 4) AS ms_played_distance_share_of_distinct_listener_id,
        ROUND(first_ms_played / NULLIF(SUM(first_ms_played) OVER (PARTITION BY distinct_listener_id_in_source_device), 0), 4) AS first_ms_played_share_of_distinct_listener_id_in
    FROM metrics
    WINDOW w_distinct_listener_id_in_sour AS (PARTITION BY distinct_listener_id_in_source_device)
)
SELECT
    id,
    listener_id,
    ms_played,
    device,
    is_skipped,
    source,
    ms_played_change,
    first_ms_played,
    ms_played_distance_vs_source_device_avg,
    first_known_ms_played,
    second_first_ms_played,
    distinct_listener_id_in_source_device_avg_ms_pla,
    ms_played_distance_share_of_distinct_listener_id,
    first_ms_played_share_of_distinct_listener_id_in,
    SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
    CASE WHEN source IN ('playlist', 'search') THEN 'group_a' WHEN source = 'album' THEN 'group_b' ELSE 'other' END AS source_group
FROM windowed
ORDER BY source DESC NULLS FIRST
