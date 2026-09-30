WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        HOUR(event_at) AS event_at_hour
    FROM ticket_events
    WHERE ticket_id >= 54
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM base
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS starts_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(starts_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        actor,
        session_no,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS event_count,
        SUM(event_at_hour) AS session_event_at_hour,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, session_no
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        session_no,
        MAX(session_no) AS max_session_no,
        ROUND(SUM(event_count) / NULLIF(SUM(session_no), 0), 3) AS event_count_to_session_no_ratio,
        SUM(CASE WHEN actor = 'agent' THEN 1 ELSE 0 END) AS n_actor_agent,
        ROUND(AVG(session_event_at_hour), 2) AS avg_session_event_at_hour,
        SUM(duration_min) AS total_duration_min,
        MIN(session_event_at_hour) AS min_session_event_at_hour
    FROM session_summary
    GROUP BY DATE_TRUNC('MONTH', session_start), session_no
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN n_actor_agent >= 7 THEN 'A' WHEN n_actor_agent >= 4 THEN 'B' ELSE 'C' END AS n_actor_agent_tier,
        ROUND(total_duration_min * 1.1, 2) AS total_duration_min_adj
    FROM stats
)
SELECT
    session_start_month,
    session_no,
    n_actor_agent,
    avg_session_event_at_hour,
    total_duration_min,
    min_session_event_at_hour,
    n_actor_agent_tier,
    DATEDIFF(DATE '2025-12-31', session_start_month) AS days_since_session_start_month
FROM enriched
WHERE n_actor_agent <= 8
