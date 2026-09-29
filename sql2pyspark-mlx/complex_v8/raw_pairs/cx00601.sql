WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag
    FROM plays
    WHERE (track_id IS NOT NULL OR listener_id <= 2)
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        ROUND(AVG(ms_played), 2) AS avg_play_ms_played,
        SUM(ms_played) AS total_play_ms_played
    FROM plays
    GROUP BY track_id
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.avg_play_ms_played,
        p.total_play_ms_played
    FROM prepared s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
play_stats_2 AS (
    -- Per-listener plays metrics
    SELECT listener_id, COUNT(*) AS play_count, COUNT(*) AS play_count_2
    FROM plays
    GROUP BY listener_id
),
profiled AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_count, 0) AS play_count,
        COALESCE(p.play_count_2, 0) AS play_count_2
    FROM with_counts s
    LEFT JOIN play_stats_2 p ON p.listener_id = s.listener_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(play_count_2, 2) OVER (ORDER BY play_count_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_play_count_2,
        ROUND(CUME_DIST() OVER (ORDER BY avg_play_ms_played, id), 3) AS avg_play_ms_played_cume_dist,
        RANK() OVER w_all_by_avg_play_ms_played_ti AS avg_play_ms_played_rank,
        COUNT(*) OVER (PARTITION BY track_id) AS rows_in_track_id,
        ROW_NUMBER() OVER (ORDER BY play_count_2, id) AS seq_no
    FROM profiled
    WINDOW w_all_by_avg_play_ms_played_ti AS (ORDER BY avg_play_ms_played)
)
-- Subtotals with CUBE
SELECT
    is_skipped_flag,
    seq_no,
    ARRAY_SORT(COLLECT_SET(device)) AS device_list
FROM with_trends
GROUP BY CUBE(is_skipped_flag, seq_no)
