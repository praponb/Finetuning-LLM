WITH scoped AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE listener_id IS NOT NULL
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM scoped
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        listener_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(ms_played) AS session_ms_played,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_id
)
-- Final projection
SELECT
    listener_id,
    session_id,
    session_start,
    session_end,
    n_events,
    session_ms_played,
    duration_min,
    n_events + session_ms_played AS n_events_plus_session_ms_played
FROM sessions
ORDER BY session_id
