WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        CONCAT_WS('-', source, device) AS source_device_label,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE ms_played < 333795
),
filtered_2 AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id <= 7
        )
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        source_device_label,
        ms_played_bucket,
        SIZE(COLLECT_SET(device) OVER w_source_track_id) AS distinct_device_in_source_track_id,
        SUM(ms_played) OVER (PARTITION BY listener_id ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_sum_3,
        NTILE(4) OVER (PARTITION BY listener_id ORDER BY ms_played DESC NULLS FIRST, id) AS ms_played_quartile,
        COUNT(*) OVER (PARTITION BY source, track_id) AS rows_in_source_track_id,
        NTH_VALUE(ms_played_bucket, 2) OVER (PARTITION BY listener_id ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_bucket
    FROM filtered_2
    WINDOW w_source_track_id AS (PARTITION BY source, track_id)
)
-- Subtotals with ROLLUP
SELECT
    source,
    ms_played_quartile,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(STDDEV(rows_in_source_track_id), 2) AS stddev_rows_in_source_track_id
FROM with_trends
GROUP BY ROLLUP(source, ms_played_quartile)
ORDER BY source NULLS LAST, ms_played_quartile NULLS LAST
