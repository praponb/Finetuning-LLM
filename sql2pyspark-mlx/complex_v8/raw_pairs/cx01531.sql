WITH prepared_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        played_at + INTERVAL 2 HOURS AS played_at_plus_2h,
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE track_id IS NOT NULL
      AND NOT is_skipped
),
device_stats AS (
    -- Avg ms_played per device
    SELECT device, AVG(ms_played) AS avg_ms_played
    FROM prepared_plays
    GROUP BY device
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_ms_played,
        ROUND(s.ms_played - g.avg_ms_played, 2) AS ms_played_minus_avg
    FROM prepared_plays s
    JOIN device_stats g ON g.device = s.device
    WHERE s.ms_played > g.avg_ms_played
)
-- Subtotals with CUBE
SELECT
    COALESCE(device, 'ALL') AS device,
    track_id,
    ROUND(AVG(is_skipped_flag), 2) AS avg_is_skipped_flag
FROM compared
GROUP BY CUBE(device, track_id)
ORDER BY device NULLS LAST, track_id NULLS LAST
