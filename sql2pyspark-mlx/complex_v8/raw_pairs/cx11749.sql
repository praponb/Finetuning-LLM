WITH covered AS (
    -- Events that happened while the ticket was open
    SELECT
        e.id,
        e.ticket_id,
        e.event_at,
        e.event_type,
        t.priority,
        t.agent_id,
        ROUND((UNIX_TIMESTAMP(e.event_at) - UNIX_TIMESTAMP(t.opened_at)) / 3600, 2) AS hours_since_open
    FROM ticket_events e
    JOIN tickets t
      ON t.id = e.ticket_id
     AND e.event_at >= t.opened_at
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 7 DAYS)
),
ticket_stats AS (
    -- Per-agent tickets metrics
    SELECT agent_id, MIN(csat) AS min_ticket_csat
    FROM tickets
    GROUP BY agent_id
),
profiled AS (
    -- Attach tickets metrics
    SELECT
        s.*,
        t.min_ticket_csat
    FROM covered s
    LEFT JOIN ticket_stats t ON t.agent_id = s.agent_id
)
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    min_ticket_csat,
    DATEDIFF(DATE '2025-06-30', event_at) AS days_since_event_at,
    CASE WHEN priority IN ('P4', 'P2') THEN 'group_a' WHEN priority = 'P3' THEN 'group_b' ELSE 'other' END AS priority_group
FROM profiled
ORDER BY agent_id NULLS LAST, days_since_event_at DESC
