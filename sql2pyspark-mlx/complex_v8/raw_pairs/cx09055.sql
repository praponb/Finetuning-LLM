WITH recent AS (
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
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE is_skipped = TRUE
),
with_prev AS (
    -- Previous played_at per listener_id
    SELECT *, LAG(played_at) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS prev_played_at
    FROM recent
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_played_at IS NULL OR played_at > prev_played_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY listener_id ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        listener_id,
        session_id,
        MIN(played_at) AS session_start,
        MAX(played_at) AS session_end,
        COUNT(*) AS n_events,
        SUM(days_since_played_at) AS session_days_since_played_at,
        ROUND((UNIX_TIMESTAMP(MAX(played_at)) - UNIX_TIMESTAMP(MIN(played_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY listener_id, session_id
),
with_parent_listener AS (
    -- Join listeners
    SELECT s.*, l.plan AS listener_plan, l.username AS listener_username
    FROM session_summary s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
SELECT
    listener_id,
    session_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    listener_plan,
    listener_username,
    ROUND(duration_min * 1.1, 2) AS duration_min_adj,
    LENGTH(TRIM(listener_username)) AS listener_username_len
FROM with_parent_listener
ORDER BY session_start NULLS LAST
