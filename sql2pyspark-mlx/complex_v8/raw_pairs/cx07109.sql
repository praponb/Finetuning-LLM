WITH base AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        INITCAP(TRIM(actor)) AS actor_clean
    FROM ticket_events
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 60 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 60 MINUTES THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        actor,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, burst_id
),
totals AS (
    -- Summarise per group
    SELECT
        events,
        TO_DATE(session_end) AS session_end_day,
        actor,
        SUM(duration_min) FILTER (WHERE actor = 'agent') AS duration_min_actor_agent,
        COUNT(*) FILTER (WHERE actor = 'agent') AS n_actor_agent,
        MAX(duration_min) AS max_duration_min,
        SUM(burst_id) AS total_burst_id
    FROM sessions
    GROUP BY events, TO_DATE(session_end), actor
    HAVING AVG(burst_id) > 2
)
-- Final projection
SELECT
    events,
    session_end_day,
    actor,
    duration_min_actor_agent,
    n_actor_agent,
    max_duration_min,
    total_burst_id,
    ROUND(events / NULLIF(n_actor_agent, 0), 3) AS events_per_n_actor_agent
FROM totals
WHERE events <= 6
ORDER BY n_actor_agent, events, session_end_day, actor
LIMIT 20
