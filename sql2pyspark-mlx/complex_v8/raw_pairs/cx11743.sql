WITH scoped_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM scoped_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
per_session AS (
    -- One row per session
    SELECT
        actor,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_no
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM per_session
    WHERE session_no < (SELECT MAX(session_no) FROM per_session)
),
totals AS (
    -- Group-level totals
    SELECT
        session_no,
        n_events,
        TO_DATE(session_start) AS session_start_day,
        COUNT(*) AS row_count,
        SUM(CASE WHEN actor <> 'system' THEN n_events ELSE 0 END) AS n_events_actor_system,
        ROUND(100 * COUNT(CASE WHEN actor = 'system' THEN 1 END) / COUNT(*), 1) AS pct_actor_system
    FROM with_activity
    GROUP BY session_no, n_events, TO_DATE(session_start)
)
SELECT
    session_no,
    n_events,
    session_start_day,
    pct_actor_system
FROM totals
