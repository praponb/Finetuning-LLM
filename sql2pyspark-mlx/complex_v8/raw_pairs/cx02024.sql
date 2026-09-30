WITH prepared AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        CONCAT_WS('-', source, device) AS source_device_label,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM plays
    WHERE (MONTH(played_at) IN (1, 3) OR source = 'radio')
),
stats AS (
    -- Summarise per group
    SELECT
        device,
        track_id,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        MAX(played_at) AS last_played_at,
        MIN(played_at) AS first_played_at,
        SUM(is_skipped_flag) FILTER (WHERE device IN ('car', 'phone')) AS is_skipped_flag_device_in_car_phone,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids
    FROM prepared
    GROUP BY device, track_id, DATE_FORMAT(played_at, 'yyyy-MM')
),
exploded_detail_plays AS (
    -- One row per play
    SELECT
        s.device,
        s.track_id,
        s.played_at_ym,
        s.last_played_at,
        s.first_played_at,
        s.is_skipped_flag_device_in_car_phone,
        s.distinct_listener_ids,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played
    FROM stats s
    LEFT JOIN plays p ON p.track_id = s.track_id
)
-- Subtotals with CUBE
SELECT
    play_listener_id,
    COALESCE(device, 'ALL') AS device,
    is_skipped_flag_device_in_car_phone,
    COUNT(DISTINCT device) AS distinct_devices,
    COUNT(*) AS row_count,
    MAX(distinct_listener_ids) AS max_distinct_listener_ids
FROM exploded_detail_plays
GROUP BY CUBE(play_listener_id, device, is_skipped_flag_device_in_car_phone)
ORDER BY play_listener_id NULLS LAST, device NULLS LAST, is_skipped_flag_device_in_car_phone NULLS LAST
