WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-12-31', played_at) AS days_since_played_at
    FROM plays
    WHERE ms_played > 56848
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(days_since_played_at) OVER w_source AS source_sum_days_since_played_at,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, source ORDER BY days_since_played_at DESC), 3) AS days_since_played_at_cume_dist,
        MIN(days_since_played_at) OVER (PARTITION BY source, is_skipped) AS source_is_skipped_min_days_since_played_at,
        SIZE(COLLECT_SET(device) OVER (PARTITION BY source, is_skipped)) AS distinct_device_in_source_is_skipped,
        LAST_VALUE(days_since_played_at) IGNORE NULLS OVER (PARTITION BY device ORDER BY days_since_played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_days_since_played_at,
        ROUND(CUME_DIST() OVER (ORDER BY days_since_played_at ASC NULLS LAST, id), 3) AS days_since_played_at_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY is_skipped ORDER BY days_since_played_at ASC) AS days_since_played_at_dense_rank,
        ROUND(days_since_played_at - AVG(days_since_played_at) OVER w_source, 2) AS days_since_played_at_vs_source_avg,
        ROUND(AVG(days_since_played_at) OVER (PARTITION BY source), 2) AS source_avg_days_since_played_at,
        days_since_played_at - LAG(days_since_played_at) OVER (PARTITION BY listener_id, track_id ORDER BY days_since_played_at ASC, id) AS days_since_played_at_change,
        days_since_played_at - LAG(days_since_played_at) OVER (PARTITION BY device ORDER BY days_since_played_at DESC NULLS FIRST, id) AS days_since_played_at_change_2,
        RANK() OVER (PARTITION BY device ORDER BY days_since_played_at DESC NULLS FIRST) AS days_since_played_at_rank
    FROM prepared_plays
    WINDOW w_source AS (PARTITION BY source)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        source,
        track_id,
        is_skipped,
        MIN(source_sum_days_since_played_at) AS min_source_sum_days_since_played_at,
        COUNT(*) FILTER (WHERE source IS NOT NULL) AS n_source_is_not_null,
        ROUND(SUM(days_since_played_at_cume_dist_2), 2) AS total_days_since_played_at_cume_dist_2,
        COUNT(*) AS cnt
    FROM with_trends
    GROUP BY source, track_id, is_skipped
)
SELECT
    source,
    track_id,
    is_skipped,
    min_source_sum_days_since_played_at,
    total_days_since_played_at_cume_dist_2,
    cnt
FROM stats
ORDER BY min_source_sum_days_since_played_at, source DESC
