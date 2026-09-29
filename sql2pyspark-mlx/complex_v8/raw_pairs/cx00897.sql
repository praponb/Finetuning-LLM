WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        LOWER(REGEXP_REPLACE(TRIM(source), ' ', '_')) AS source_slug
    FROM plays
    WHERE source NOT IN ('playlist', 'album')
      AND is_skipped = FALSE
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        played_at,
        ms_played,
        device,
        COUNT(*) OVER (PARTITION BY device ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        MIN(ms_played) OVER (PARTITION BY listener_id) AS listener_id_min_ms_played,
        ROUND(ms_played_filled - AVG(ms_played_filled) OVER (PARTITION BY listener_id), 2) AS ms_played_filled_vs_listener_id_avg,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY listener_id)) AS distinct_source_in_listener_id,
        DENSE_RANK() OVER w_device_by_ms_played_ties AS ms_played_dense_rank
    FROM recent_plays
    WINDOW w_listener_id AS (PARTITION BY listener_id), w_device_by_ms_played_ties AS (PARTITION BY device ORDER BY ms_played)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        ms_played,
        device,
        listener_id_min_ms_played,
        ms_played_filled_vs_listener_id_avg,
        ms_played_dense_rank,
        MAX(running_count) OVER w_device_distinct_source_in_li AS device_distinct_source_in_listener_id_max_runnin,
        LAG(running_count) OVER (PARTITION BY distinct_source_in_listener_id ORDER BY listener_id_min_ms_played, id) AS prev_running_count,
        SUM(running_count) OVER (PARTITION BY distinct_source_in_listener_id ORDER BY listener_id_min_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_running_count,
        ROUND(AVG(ms_played_dense_rank) OVER (PARTITION BY device, distinct_source_in_listener_id), 2) AS device_distinct_source_in_listener_id_avg_ms_pla,
        COUNT(*) OVER (PARTITION BY device, distinct_source_in_listener_id) AS rows_in_device_distinct_source_in_listener_id
    FROM metrics
    WINDOW w_device_distinct_source_in_li AS (PARTITION BY device, distinct_source_in_listener_id)
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM windowed
    WHERE ms_played_dense_rank > (SELECT AVG(ms_played_dense_rank) FROM windowed)
),
unique_rows AS (
    -- Keep the latest row per device
    SELECT
        id,
        ms_played,
        device,
        listener_id_min_ms_played,
        ms_played_filled_vs_listener_id_avg,
        ms_played_dense_rank,
        device_distinct_source_in_listener_id_max_runnin,
        prev_running_count,
        running_running_count,
        device_distinct_source_in_listener_id_avg_ms_pla,
        rows_in_device_distinct_source_in_listener_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device ORDER BY device_distinct_source_in_listener_id_avg_ms_pla DESC, id DESC) AS recency_rank
        FROM with_activity
    ) ranked
    WHERE recency_rank = 1
),
device_stats AS (
    -- Avg running_running_count per device
    SELECT device, AVG(running_running_count) AS avg_running_running_count
    FROM unique_rows
    GROUP BY device
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_running_running_count,
        ROUND(s.running_running_count - g.avg_running_running_count, 2) AS running_running_count_minus_avg
    FROM unique_rows s
    JOIN device_stats g ON g.device = s.device
    WHERE s.running_running_count > g.avg_running_running_count
)
-- Subtotals with GROUPING SETS
SELECT
    device,
    ms_played_dense_rank,
    MAX(running_running_count) AS max_running_running_count
FROM vs_group
GROUP BY GROUPING SETS ((device, ms_played_dense_rank), (device))
ORDER BY device NULLS LAST, ms_played_dense_rank NULLS LAST
