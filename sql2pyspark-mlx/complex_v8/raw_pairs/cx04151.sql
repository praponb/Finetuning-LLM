WITH filtered AS (
    -- Scope the plays rows
    SELECT *
    FROM plays
    WHERE HOUR(played_at) BETWEEN 8 AND 11
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR (UNIX_TIMESTAMP(played_at) - UNIX_TIMESTAMP(prev_played_at)) / 60 > 90 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        track_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, burst_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(ms_played) AS max_play_ms_played,
        MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        p.last_played_at
    FROM sessions s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        track_id,
        burst_id,
        session_start,
        session_end,
        n_events,
        max_play_ms_played,
        last_played_at,
        ROUND(n_events / NULLIF(SUM(n_events) OVER (PARTITION BY track_id, MONTH(session_start)), 0), 4) AS n_events_share_of_track_id_session_start_month,
        MAX(n_events) OVER (PARTITION BY track_id, MONTH(session_start)) AS track_id_session_start_month_max_n_events,
        ROUND(AVG(duration_min) OVER w_track_id_session_start_month, 2) AS track_id_session_start_month_avg_duration_min,
        LAG(burst_id) OVER (PARTITION BY track_id, burst_id ORDER BY session_start, track_id, burst_id) AS prev_burst_id,
        ROUND(n_events - AVG(n_events) OVER w_track_id_session_start_month, 2) AS n_events_vs_track_id_session_start_month_avg
    FROM with_stats
    WINDOW w_track_id_session_start_month AS (PARTITION BY track_id, MONTH(session_start))
)
-- Final projection
SELECT
    track_id,
    burst_id,
    session_start,
    session_end,
    n_events,
    n_events_vs_track_id_session_start_month_avg,
    ABS(prev_burst_id - 4) AS prev_burst_id_distance,
    FLOOR(max_play_ms_played / 50000) * 50000 AS max_play_ms_played_bucket
FROM ranked
ORDER BY session_start DESC
