WITH filtered_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        COALESCE(ms_played, 0) AS ms_played_filled,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE track_id >= 5
),
device_stats AS (
    -- Avg ms_played_filled per device
    SELECT device, AVG(ms_played_filled) AS avg_ms_played_filled
    FROM filtered_plays
    GROUP BY device
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played_filled,
        ROUND(s.ms_played_filled - g.avg_ms_played_filled, 2) AS ms_played_filled_minus_avg
    FROM filtered_plays s
    JOIN device_stats g ON g.device = s.device
    WHERE s.ms_played_filled > g.avg_ms_played_filled
)
SELECT
    is_skipped,
    listener_id,
    device,
    SUM(ms_played_filled) AS total_ms_played_filled,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list
FROM outliers
GROUP BY CUBE(is_skipped, listener_id, device)
ORDER BY is_skipped NULLS LAST, listener_id NULLS LAST, device NULLS LAST
