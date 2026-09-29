WITH matched AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
scored AS (
    -- Add calculated columns
    SELECT *, QUARTER(event_at) AS event_at_quarter
    FROM matched
    WHERE agent_id <= 8
)
-- Final projection
SELECT
    id,
    ticket_id,
    priority,
    agent_id,
    hours_since_open,
    event_at_quarter
FROM scored
WHERE agent_id IS NOT NULL
ORDER BY priority, event_at_quarter
