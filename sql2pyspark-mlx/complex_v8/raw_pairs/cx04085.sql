WITH filtered_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        ABS(ms_played - 151112) AS ms_played_distance,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE source NOT IN ('radio', 'album')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        device,
        is_skipped,
        source,
        played_at_label,
        COUNT(*) OVER (PARTITION BY listener_id) AS rows_in_listener_id,
        DENSE_RANK() OVER (PARTITION BY source, device ORDER BY ms_played_distance ASC) AS ms_played_distance_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source, listener_id ORDER BY ms_played, id), 3) AS ms_played_percent_rank,
        ms_played - LAG(ms_played) OVER w_source_by_ms_played AS ms_played_change,
        ROUND(AVG(ms_played) OVER (PARTITION BY source, device ORDER BY ms_played_distance ASC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_3_prior,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id), 2) AS ms_played_vs_listener_id_avg,
        MAX(ms_played) OVER (PARTITION BY listener_id) AS listener_id_max_ms_played,
        COUNT(*) OVER (PARTITION BY source, device ORDER BY ms_played_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM filtered_plays
    WINDOW w_source_by_ms_played AS (PARTITION BY source ORDER BY ms_played, id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE ms_played_distance_dense_rank <= 5
      AND NOT is_skipped
),
per_group AS (
    -- Summarise per group
    SELECT
        listener_id,
        CASE WHEN ms_played_change >= 160000 THEN 'A' WHEN ms_played_change >= -160000 THEN 'B' ELSE 'C' END AS ms_played_change_tier,
        ms_played_distance_dense_rank,
        ROUND(STDDEV(listener_id_max_ms_played), 2) AS stddev_listener_id_max_ms_played,
        ROUND(SUM(listener_id_max_ms_played) / NULLIF(SUM(ms_played_vs_listener_id_avg), 0), 3) AS listener_id_max_ms_played_to_ms_played_vs_listen,
        ROUND(STDDEV(ms_played_percent_rank), 2) AS stddev_ms_played_percent_rank,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        SUM(running_count) AS total_running_count,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list
    FROM top_rows
    GROUP BY 1, 2, 3
)
SELECT
    listener_id,
    ms_played_change_tier,
    ms_played_distance_dense_rank,
    listener_id_max_ms_played_to_ms_played_vs_listen,
    stddev_ms_played_percent_rank,
    device_list,
    source_list,
    LOWER(REGEXP_REPLACE(TRIM(ms_played_change_tier), ' ', '_')) AS ms_played_change_tier_slug
FROM per_group
ORDER BY ms_played_change_tier_slug, listener_id DESC NULLS FIRST, ms_played_change_tier, ms_played_distance_dense_rank
LIMIT 20
