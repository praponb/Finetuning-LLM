WITH recent_events AS (
    -- Scope the ticket_events rows
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
    FROM recent_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
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
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', session_end) AS session_end_month,
        n_events,
        ROUND(SUM(n_events) / NULLIF(SUM(session_no), 0), 3) AS n_events_to_session_no_ratio,
        SUM(session_no) AS total_session_no,
        COUNT(*) AS row_count,
        ROUND(SUM(session_no) / NULLIF(SUM(duration_min), 0), 3) AS session_no_to_duration_min_ratio,
        COUNT(DISTINCT actor) AS distinct_actors,
        COUNT(*) FILTER (WHERE actor IN ('customer', 'system')) AS n_actor_in_customer_system
    FROM session_summary
    GROUP BY DATE_TRUNC('MONTH', session_end), n_events
)
SELECT session_end_month, n_events, measure, amount
FROM (
    SELECT session_end_month, n_events, n_actor_in_customer_system, row_count, distinct_actors
    FROM stats
) src
UNPIVOT (
    amount FOR measure IN (n_actor_in_customer_system, row_count, distinct_actors)
) unp
