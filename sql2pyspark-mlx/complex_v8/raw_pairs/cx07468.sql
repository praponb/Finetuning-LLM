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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 1 DAYS)
),
enriched_ticket AS (
    -- Attach ticket attributes
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.opened_at AS ticket_opened_at,
        su.tier AS agent_tier,
        su.agent_name
    FROM covered s
    LEFT JOIN tickets t ON t.id = s.ticket_id
    LEFT JOIN support_agents su ON su.id = t.agent_id
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    hours_since_open,
    ticket_opened_at,
    agent_name
FROM enriched_ticket
WHERE event_at < TIMESTAMP '2025-02-24 08:00:00'
ORDER BY ticket_opened_at DESC, event_at
