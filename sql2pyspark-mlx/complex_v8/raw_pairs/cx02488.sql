WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT *
    FROM plays
    WHERE ms_played >= 168782
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        listener_id,
        burst_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, burst_id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(duration_min, 2) OVER (PARTITION BY event_count, burst_id ORDER BY session_end ASC, listener_id, burst_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_min,
        LAG(event_count, 3) OVER (ORDER BY session_start, listener_id, burst_id) AS prev3_event_count
    FROM per_session
)
SELECT
    listener_id,
    burst_id,
    session_start,
    event_count,
    session_ms_played,
    second_duration_min,
    prev3_event_count,
    ROUND(duration_min * 1.1, 2) AS duration_min_adj,
    DATEDIFF(DATE '2025-12-31', session_start) AS days_since_session_start
FROM with_windows
