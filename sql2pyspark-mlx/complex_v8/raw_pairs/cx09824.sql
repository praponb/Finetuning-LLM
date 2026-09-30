WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped,
        source,
        IF(is_skipped, 'yes', 'no') AS is_skipped_label,
        CASE WHEN device IN ('car', 'tv') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group,
        CONCAT_WS('|', source, device) AS source_device_label
    FROM plays
    WHERE track_id IS NOT NULL
      AND is_skipped = TRUE
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY listener_id, device), 2) AS ms_played_vs_listener_id_device_avg,
        ROUND(AVG(ms_played) OVER w_listener_id_device, 2) AS listener_id_device_avg_ms_played,
        ROUND(CUME_DIST() OVER w_device_group_listener_id_by, 3) AS ms_played_cume_dist,
        ROUND(AVG(ms_played) OVER (PARTITION BY device_group, listener_id ORDER BY ms_played NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_2,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY listener_id, device)) AS distinct_source_in_listener_id_device
    FROM recent_plays
    WINDOW w_listener_id_device AS (PARTITION BY listener_id, device), w_device_group_listener_id_by AS (PARTITION BY device_group, listener_id ORDER BY ms_played NULLS LAST)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        device,
        is_skipped,
        device_group,
        source_device_label,
        ms_played_vs_listener_id_device_avg,
        listener_id_device_avg_ms_played,
        ms_played_cume_dist,
        distinct_source_in_listener_id_device,
        SUM(ms_played) OVER (PARTITION BY listener_id, device_group ORDER BY ms_played_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_ms_played,
        MAX(ms_played_cume_dist) OVER (ORDER BY ms_played ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_ms_played_cume_dist_so_far,
        ROUND(ms_played_avg_2 / NULLIF(SUM(ms_played_avg_2) OVER (PARTITION BY source, track_id), 0), 4) AS ms_played_avg_2_share_of_source_track_id,
        ROW_NUMBER() OVER (PARTITION BY listener_id, device_group ORDER BY ms_played_cume_dist, id) AS rn
    FROM with_windows
)
SELECT *
FROM (
    SELECT rn, device_group, device, distinct_source_in_listener_id_device
    FROM with_trends
) src
PIVOT (
    COUNT(distinct_source_in_listener_id_device)
    FOR device IN ('phone', 'speaker')
)
ORDER BY rn, device_group
