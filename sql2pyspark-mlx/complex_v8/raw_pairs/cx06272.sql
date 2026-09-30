WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CASE WHEN actor IN ('customer', 'system') THEN 'group_a' WHEN actor = 'agent' THEN 'group_b' ELSE 'other' END AS actor_group
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 90 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 90 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
sessions AS (
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
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY session_no, actor ORDER BY session_end ASC, actor, session_no) AS session_end_tercile,
        LAG(session_start, 2) OVER w_session_no_actor_by_session AS prev2_session_start,
        RANK() OVER (PARTITION BY session_no, actor ORDER BY session_end ASC) AS session_end_rank,
        FIRST_VALUE(session_start) IGNORE NULLS OVER (PARTITION BY session_no, actor ORDER BY session_end ASC, actor, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_session_start
    FROM sessions
    WINDOW w_session_no_actor_by_session AS (PARTITION BY session_no, actor ORDER BY session_end ASC, actor, session_no)
)
-- Final projection
SELECT
    actor,
    session_no,
    session_start,
    duration_min,
    session_end_tercile,
    prev2_session_start,
    session_end_rank,
    first_known_session_start,
    COALESCE(duration_min, 0.0) AS duration_min_filled
FROM with_windows
WHERE session_end_rank BETWEEN 3 AND 8
ORDER BY duration_min_filled, first_known_session_start NULLS LAST
