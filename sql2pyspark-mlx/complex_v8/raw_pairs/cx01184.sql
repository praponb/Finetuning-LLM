WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CONCAT(source, '#', CAST(ms_played AS STRING)) AS source_ref,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE (ms_played > 49807 OR played_at < TIMESTAMP '2025-02-18 10:00:00')
),
joined AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        t.release_date AS track_release_date,
        t.duration_sec AS track_duration_sec
    FROM recent s
    LEFT JOIN tracks t ON t.id = s.track_id
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at) ORDER BY track_release_date DESC), 3) AS track_release_date_cume_dist,
        COUNT(*) OVER (PARTITION BY listener_id, track_id ORDER BY track_duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        ROUND(AVG(track_duration_sec) OVER (ORDER BY track_release_date NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS track_duration_sec_avg_2,
        ROUND(AVG(track_duration_sec) OVER (PARTITION BY device ORDER BY track_duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS track_duration_sec_avg_2_prior,
        MAX(track_duration_sec) OVER (PARTITION BY device, listener_id) AS device_listener_id_max_track_duration_sec,
        ROUND(track_duration_sec - AVG(track_duration_sec) OVER (PARTITION BY listener_id, device), 2) AS track_duration_sec_vs_listener_id_device_avg,
        ROW_NUMBER() OVER (PARTITION BY listener_id, DATE_TRUNC('MONTH', played_at) ORDER BY track_release_date DESC, id) AS rn,
        NTILE(3) OVER w_device_by_track_duration_sec_2 AS track_duration_sec_tercile,
        SIZE(COLLECT_SET(listener_id) OVER (PARTITION BY device)) AS distinct_listener_id_in_device
    FROM joined
    WINDOW w_device_by_track_duration_sec_2 AS (PARTITION BY device ORDER BY track_duration_sec, id)
),
per_group AS (
    -- Group-level totals
    SELECT
        track_duration_sec_tercile,
        rn,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(DISTINCT device) AS distinct_devices,
        MIN(device_listener_id_max_track_duration_sec) AS min_device_listener_id_max_track_duration_sec,
        SUM(running_count) AS total_running_count,
        ROUND(STDDEV(track_duration_sec_avg_2_prior), 2) AS stddev_track_duration_sec_avg_2_prior
    FROM windowed
    GROUP BY 1, 2
    HAVING MAX(track_release_date_cume_dist) > 0.2
)
-- Final projection
SELECT
    track_duration_sec_tercile,
    rn,
    distinct_listener_ids,
    distinct_devices,
    min_device_listener_id_max_track_duration_sec,
    total_running_count,
    stddev_track_duration_sec_avg_2_prior,
    distinct_devices + distinct_listener_ids AS distinct_devices_plus_distinct_listener_ids
FROM per_group
WHERE track_duration_sec_tercile <= 9
ORDER BY track_duration_sec_tercile DESC NULLS FIRST, rn
LIMIT 10
