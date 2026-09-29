WITH filtered_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
    WHERE TO_DATE(played_at) < DATE '2025-02-06'
),
listener_id_stats AS (
    -- Median ms_played per listener_id
    SELECT listener_id, PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM filtered_plays
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_ms_played,
        ROUND(s.ms_played - g.median_ms_played, 2) AS ms_played_minus_median
    FROM filtered_plays s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.ms_played >= g.median_ms_played
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.listener_id = s.listener_id) AS first_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.listener_id = s.listener_id) AS play_count
    FROM vs_group s
),
combined_details AS (
    -- Join tracks
    SELECT s.*, t.duration_sec AS track_duration_sec
    FROM summarized_rows s
    JOIN tracks t ON t.id = s.track_id
)
-- Subtotals with GROUPING SETS
SELECT
    device,
    is_skipped,
    GROUPING(device) AS is_total_device,
    SUM(CASE WHEN device IN ('desktop', 'phone') THEN 1 ELSE 0 END) AS n_device_in_desktop_phone,
    MAX(median_ms_played) AS max_median_ms_played,
    ROUND(AVG(play_count), 2) AS avg_play_count
FROM combined_details
GROUP BY GROUPING SETS ((device, is_skipped), (device), (is_skipped))
