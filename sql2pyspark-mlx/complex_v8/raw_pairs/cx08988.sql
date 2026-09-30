WITH cleaned AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE is_skipped = TRUE
      AND source = 'search'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(LAG(played_at) OVER w_listener_id_by_ms_played)) / 60, 1) AS minutes_since_prev_played_at,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY track_id, device ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at,
        MAX(ms_played) OVER (PARTITION BY track_id, device) AS track_id_device_max_ms_played,
        ROUND(CUME_DIST() OVER w_track_id_device_by_ms_played_2, 3) AS ms_played_cume_dist,
        COUNT(*) OVER (PARTITION BY source) AS rows_in_source,
        LAG(played_at, 3) OVER (ORDER BY played_at NULLS LAST, id) AS prev3_played_at,
        NTILE(10) OVER (PARTITION BY track_id, device ORDER BY ms_played ASC, id) AS ms_played_decile,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id, device)) AS distinct_listener_id_in_track_id_device,
        ROUND(PERCENT_RANK() OVER w_track_id_device_by_ms_played_2, 3) AS ms_played_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id ORDER BY ms_played ASC), 3) AS ms_played_cume_dist_2,
        ROW_NUMBER() OVER (PARTITION BY track_id, device ORDER BY ms_played ASC, id) AS seq_no,
        LEAD(ms_played) OVER (PARTITION BY device ORDER BY ms_played DESC, id) AS next_ms_played
    FROM cleaned
    WINDOW w_listener_id_by_ms_played AS (PARTITION BY listener_id ORDER BY ms_played ASC, id), w_track_id_device_by_ms_played_2 AS (PARTITION BY track_id, device ORDER BY ms_played ASC)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(next_ms_played) OVER (PARTITION BY source, MONTH(prev3_played_at) ORDER BY ms_played_cume_dist ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS next_ms_played_avg_4,
        SUM(ms_played_decile) OVER (PARTITION BY track_id) AS track_id_sum_ms_played_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY device, ms_played_decile ORDER BY ms_played_cume_dist_2, id), 3) AS ms_played_cume_dist_2_percent_rank,
        ROW_NUMBER() OVER w_device_ms_played_decile_by_m AS rn,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id
    FROM sequenced
    WINDOW w_device_ms_played_decile_by_m AS (PARTITION BY device, ms_played_decile ORDER BY ms_played_cume_dist_2, id)
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(last_known_played_at, 'yyyy-MM') AS last_known_played_at_ym,
        seq_no,
        MAX(last_known_played_at) AS last_last_known_played_at,
        ARRAY_SORT(COLLECT_SET(device)) AS device_list,
        COUNT(*) AS cnt,
        ROUND(STDDEV(distinct_listener_id_in_track_id_device), 2) AS stddev_distinct_listener_id_in_track_id_device,
        COUNT(DISTINCT source) AS distinct_sources
    FROM ranked
    GROUP BY DATE_FORMAT(last_known_played_at, 'yyyy-MM'), seq_no
)
SELECT
    last_known_played_at_ym,
    seq_no,
    last_last_known_played_at,
    device_list,
    cnt,
    distinct_sources
FROM grouped
