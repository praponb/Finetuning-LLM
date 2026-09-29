WITH prepared AS (
    -- Scope the ticket_events rows
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor
    FROM ticket_events
    WHERE ticket_id IS NOT NULL
),
with_prev AS (
    -- Previous event_at per actor
    SELECT *, LAG(event_at) OVER (PARTITION BY actor ORDER BY event_at, id) AS prev_event_at
    FROM prepared
),
flagged AS (
    -- Flag rows that start a new session (gap > 30 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR event_at > prev_event_at + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY actor ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
session_summary AS (
    -- One row per session
    SELECT
        actor,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY actor, burst_id
),
stats AS (
    -- Group-level totals
    SELECT
        n_events,
        burst_id,
        DATE_TRUNC('MONTH', session_start) AS session_start_month,
        SUM(duration_min) AS total_duration_min,
        ROUND(SUM(n_events) / NULLIF(SUM(burst_id), 0), 3) AS n_events_to_burst_id_ratio,
        MAX(burst_id) AS max_burst_id,
        ROUND(AVG(n_events), 2) AS avg_n_events,
        MIN(session_end) AS first_session_end,
        SUM(CASE WHEN actor = 'agent' THEN n_events ELSE 0 END) AS n_events_actor_agent
    FROM session_summary
    GROUP BY 1, 2, 3
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(max_burst_id - AVG(max_burst_id) OVER (PARTITION BY burst_id), 2) AS max_burst_id_vs_burst_id_avg,
        LAG(n_events_to_burst_id_ratio, 2) OVER w_burst_id_by_first_session_en AS prev2_n_events_to_burst_id_ratio,
        LAST_VALUE(max_burst_id) OVER (PARTITION BY burst_id ORDER BY first_session_end, n_events, burst_id, session_start_month ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_max_burst_id
    FROM stats
    WINDOW w_burst_id_by_first_session_en AS (PARTITION BY burst_id ORDER BY first_session_end, n_events, burst_id, session_start_month)
)
-- Final projection
SELECT
    n_events,
    burst_id,
    session_start_month,
    total_duration_min,
    n_events_to_burst_id_ratio,
    avg_n_events,
    first_session_end,
    n_events_actor_agent,
    max_burst_id_vs_burst_id_avg,
    prev2_n_events_to_burst_id_ratio,
    last_max_burst_id,
    n_events_actor_agent + n_events AS n_events_actor_agent_plus_n_events
FROM ranked
ORDER BY first_session_end DESC, n_events_actor_agent_plus_n_events DESC
