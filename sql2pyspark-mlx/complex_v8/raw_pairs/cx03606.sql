WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        priority,
        channel,
        csat
    FROM tickets
),
with_prev AS (
    -- Previous closed_at per channel
    SELECT *, LAG(closed_at) OVER (PARTITION BY channel ORDER BY closed_at, id) AS prev_closed_at
    FROM scoped_tickets
),
flagged AS (
    -- Flag rows that start a new session (gap > 2 hours)
    SELECT *, CASE WHEN prev_closed_at IS NULL OR closed_at > prev_closed_at + INTERVAL 2 HOURS THEN 1 ELSE 0 END AS new_session_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_session_flag) OVER (PARTITION BY channel ORDER BY closed_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
line_level AS (
    -- One row per event
    SELECT
        s.id,
        s.agent_id,
        s.closed_at,
        s.priority,
        s.csat,
        s.prev_closed_at,
        s.session_id,
        t.id AS event_id,
        t.actor AS event_actor,
        t.event_type
    FROM sessionized s
    LEFT JOIN ticket_events t ON t.ticket_id = s.id
)
-- Final projection
SELECT
    id,
    agent_id,
    closed_at,
    priority,
    csat,
    prev_closed_at,
    session_id,
    event_id,
    event_actor,
    event_type,
    CONCAT_WS(': ', event_actor, priority) AS event_actor_priority_label,
    COALESCE(event_actor, 'UNKNOWN') AS event_actor_or_unknown
FROM line_level
ORDER BY event_actor_priority_label, event_id
