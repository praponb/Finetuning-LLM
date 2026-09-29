WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped
    FROM plays
    WHERE ms_played < 299017
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        l.username AS listener_username,
        t.bpm AS track_bpm,
        t.duration_sec AS track_duration_sec
    FROM prepared s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
listener_id_stats AS (
    -- Avg track_duration_sec per listener_id
    SELECT listener_id, AVG(track_duration_sec) AS avg_track_duration_sec
    FROM with_parent_listener
    GROUP BY listener_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_track_duration_sec,
        ROUND(s.track_duration_sec - g.avg_track_duration_sec, 2) AS track_duration_sec_minus_avg
    FROM with_parent_listener s
    JOIN listener_id_stats g ON g.listener_id = s.listener_id
    WHERE s.track_duration_sec > g.avg_track_duration_sec
)
-- Subtotals with GROUPING SETS
SELECT
    track_id,
    device,
    GROUPING(track_id) AS is_total_track_id,
    COUNT(*) AS cnt,
    ROUND(SUM(track_duration_sec) / NULLIF(SUM(track_bpm), 0), 3) AS track_duration_sec_to_track_bpm_ratio,
    COUNT(DISTINCT id) AS distinct_ids
FROM vs_group
GROUP BY GROUPING SETS ((track_id, device), (track_id), (device))
ORDER BY track_id NULLS LAST, device NULLS LAST
