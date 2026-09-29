WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        CONCAT(actor, '#', CAST(ticket_id AS STRING)) AS actor_ref
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM filtered
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 30 THEN 1 ELSE 0 END AS new_session_flag
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
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(n_events) OVER (PARTITION BY actor ORDER BY session_end DESC, actor, session_no) AS next_n_events,
        MIN(session_no) OVER (ORDER BY n_events DESC, actor, session_no ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_session_no_so_far,
        ROUND((UNIX_TIMESTAMP(session_start) - UNIX_TIMESTAMP(LAG(session_start) OVER (ORDER BY n_events DESC, actor, session_no))) / 60, 1) AS minutes_since_prev_session_start,
        MIN(n_events) OVER (PARTITION BY MONTH(session_end)) AS session_end_month_min_n_events,
        LEAD(session_no, 1, 0) OVER w_all_by_n_events AS next_session_no
    FROM sessions
    WINDOW w_all_by_n_events AS (ORDER BY n_events DESC, actor, session_no)
)
SELECT
    actor,
    session_no,
    session_end,
    duration_min,
    next_n_events,
    min_session_no_so_far,
    next_session_no,
    INITCAP(TRIM(actor)) AS actor_clean
FROM metrics
ORDER BY actor_clean DESC NULLS FIRST, min_session_no_so_far DESC
