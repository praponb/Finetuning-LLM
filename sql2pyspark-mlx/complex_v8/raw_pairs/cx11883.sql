WITH cleaned_events AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        ticket_id,
        event_at,
        event_type,
        actor,
        COALESCE(actor, 'UNKNOWN') AS actor_or_unknown
    FROM ticket_events
    WHERE ticket_id <= 12
),
with_prev AS (
    -- Previous event_at per ticket_id
    SELECT *, LAG(event_at) OVER (PARTITION BY ticket_id ORDER BY event_at, id) AS prev_event_at
    FROM cleaned_events
),
flagged AS (
    -- Flag rows that start a new session (gap > 45 minutes)
    SELECT *, CASE WHEN prev_event_at IS NULL OR (UNIX_TIMESTAMP(event_at) - UNIX_TIMESTAMP(prev_event_at)) / 60 > 45 THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY ticket_id ORDER BY event_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
sessions AS (
    -- One row per session
    SELECT
        ticket_id,
        burst_id,
        MIN(event_at) AS session_start,
        MAX(event_at) AS session_end,
        COUNT(*) AS n_events,
        ROUND((UNIX_TIMESTAMP(MAX(event_at)) - UNIX_TIMESTAMP(MIN(event_at))) / 60, 1) AS duration_min
    FROM sessionized
    GROUP BY ticket_id, burst_id
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.ticket_id,
        s.burst_id,
        s.session_start,
        s.session_end,
        s.n_events,
        s.duration_min,
        t.agent_id AS ticket_agent_id,
        t.tags AS ticket_tags,
        su.agent_name
    FROM sessions s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Final projection
SELECT
    ticket_id,
    burst_id,
    session_start,
    session_end,
    n_events,
    duration_min,
    ticket_agent_id,
    ticket_tags,
    agent_name,
    LENGTH(TRIM(agent_name)) AS agent_name_len
FROM enriched_ticket
WHERE ARRAY_CONTAINS(ticket_tags, 'bug')
ORDER BY duration_min, session_end, ticket_id, burst_id
LIMIT 20
