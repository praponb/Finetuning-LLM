WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        DATE_TRUNC('MONTH', played_at) AS played_at_month
    FROM plays
),
with_prev AS (
    -- Previous played_at per track_id
    SELECT *, LAG(played_at) OVER (PARTITION BY track_id ORDER BY played_at, id) AS prev_played_at
    FROM recent_plays
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY track_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        track_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY track_id, session_id
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        COUNT(CASE WHEN source IN ('search', 'radio') THEN 1 END) AS play_source_count
    FROM plays
    GROUP BY track_id
),
with_stats AS (
    -- Attach plays metrics
    SELECT
        s.*,
        COALESCE(p.play_source_count, 0) AS play_source_count
    FROM sessions s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
)
-- Final projection
SELECT
    track_id,
    session_start,
    event_count,
    duration_min,
    play_source_count,
    LEAST(event_count, duration_min) AS least_event_count_duration_min,
    DATE_TRUNC('DAY', session_start) AS session_start_day
FROM with_stats
ORDER BY session_start_day
