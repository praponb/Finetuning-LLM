WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        ABS(ms_played - 157900) AS ms_played_distance
    FROM plays
    WHERE listener_id <= 9
      AND MONTH(played_at) IN (1, 3)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        played_at_label,
        ms_played_distance,
        SUM(ms_played) OVER (PARTITION BY track_id) AS track_id_sum_ms_played,
        FIRST_VALUE(ms_played) IGNORE NULLS OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY track_id, listener_id)) AS distinct_source_in_track_id_listener_id,
        FIRST_VALUE(ms_played_distance) IGNORE NULLS OVER (PARTITION BY source ORDER BY ms_played_distance ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_distance,
        FIRST_VALUE(ms_played) OVER (ORDER BY ms_played_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_ms_played,
        SUM(ms_played) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        MAX(ms_played_distance) OVER (PARTITION BY track_id) AS track_id_max_ms_played_distance,
        DENSE_RANK() OVER (PARTITION BY source ORDER BY ms_played_distance ASC, id) AS ms_played_distance_dense_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_track_id_listener_id, 0), 4) AS ms_played_share_of_track_id_listener_id,
        NTILE(5) OVER (PARTITION BY track_id ORDER BY ms_played ASC, id) AS ms_played_quintile,
        ROUND(PERCENT_RANK() OVER (ORDER BY ms_played), 3) AS ms_played_percent_rank,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id, listener_id), 2) AS track_id_listener_id_avg_ms_played,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY track_id)) AS distinct_listener_id_in_track_id,
        SIZE(COLLECT_SET(device) OVER w_track_id_listener_id) AS distinct_device_in_track_id_listener_id,
        MIN(ms_played_distance) OVER (PARTITION BY track_id, listener_id) AS track_id_listener_id_min_ms_played_distance
    FROM filtered_plays
    WINDOW w_track_id_listener_id AS (PARTITION BY track_id, listener_id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(first_ms_played) OVER (PARTITION BY ms_played_quintile, track_id ORDER BY track_id_max_ms_played_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_ms_played,
        RANK() OVER (PARTITION BY ms_played_quintile, track_id ORDER BY track_id_max_ms_played_distance, id) AS track_id_max_ms_played_distance_rank,
        MAX(ms_played) OVER (PARTITION BY track_id, ms_played_quintile ORDER BY track_id_listener_id_avg_ms_played DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ms_played_max_7,
        LEAD(ms_played_percent_rank) OVER w_source_ms_played_quintile_by AS next_ms_played_percent_rank,
        track_id_sum_ms_played - LAG(track_id_sum_ms_played) OVER w_track_id_ms_played_quintile_2 AS track_id_sum_ms_played_change
    FROM windowed
    WINDOW w_source_ms_played_quintile_by AS (PARTITION BY source, ms_played_quintile ORDER BY distinct_device_in_track_id_listener_id, id), w_track_id_ms_played_quintile_2 AS (PARTITION BY track_id, ms_played_quintile ORDER BY track_id_listener_id_avg_ms_played DESC NULLS FIRST, id)
)
SELECT DISTINCT distinct_source_in_track_id_listener_id, first_ms_played, device
FROM sequenced
WHERE ms_played_distance > 140000
ORDER BY distinct_source_in_track_id_listener_id DESC, device DESC
