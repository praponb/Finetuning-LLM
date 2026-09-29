WITH cleaned_plays AS (
    -- Keep valid rows and derive helper columns
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
    WHERE source = 'radio'
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(ms_played) OVER w_source, 2) AS source_avg_ms_played,
        MAX(ms_played) OVER (PARTITION BY source) AS source_max_ms_played,
        MAX(ms_played) OVER (PARTITION BY device ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_max_2_prior
    FROM cleaned_plays
    WINDOW w_source AS (PARTITION BY source)
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM sequenced s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id <= 2
        )
),
sequenced_2 AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source_avg_ms_played,
        ms_played_max_2_prior,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device, MONTH(played_at))) AS distinct_track_id_in_device_played_at_month,
        ROUND(AVG(source_max_ms_played) OVER (PARTITION BY device, track_id ORDER BY source_max_ms_played DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS source_max_ms_played_avg_7,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, track_id ORDER BY played_at DESC), 3) AS played_at_cume_dist,
        LAST_VALUE(source_max_ms_played) OVER (PARTITION BY listener_id, track_id ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_source_max_ms_played,
        RANK() OVER (PARTITION BY listener_id, track_id ORDER BY played_at DESC) AS played_at_rank,
        SUM(ms_played_max_2_prior) OVER w_device_played_at_month AS device_played_at_month_sum_ms_played_max_2_prior
    FROM with_activity
    WINDOW w_device_played_at_month AS (PARTITION BY device, MONTH(played_at))
)
-- Subtotals with GROUPING SETS
SELECT
    distinct_track_id_in_device_played_at_month,
    device,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(distinct_track_id_in_device_played_at_month), 2) AS stddev_distinct_track_id_in_device_played_at_mon,
    ROUND(SUM(source_max_ms_played_avg_7) / NULLIF(SUM(device_played_at_month_sum_ms_played_max_2_prior), 0), 3) AS source_max_ms_played_avg_7_to_device_played_at_m
FROM sequenced_2
GROUP BY GROUPING SETS ((distinct_track_id_in_device_played_at_month, device), (distinct_track_id_in_device_played_at_month))
ORDER BY distinct_track_id_in_device_played_at_month NULLS LAST, device NULLS LAST
