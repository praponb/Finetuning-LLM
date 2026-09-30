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
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        event_at + INTERVAL 2 HOURS AS event_at_plus_2h,
        DATE_FORMAT(event_at, 'MMM yyyy') AS event_at_label
    FROM covered
    WHERE event_type IN ('resolved', 'assigned', 'escalated')
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_at,
    event_type,
    priority,
    agent_id,
    event_at_plus_2h,
    event_at_label,
    DATEDIFF(DATE '2025-12-31', event_at_plus_2h) AS days_since_event_at_plus_2h,
    DATEDIFF(DATE '2025-12-31', event_at_plus_2h) AS days_since_event_at_plus_2h_2
FROM enriched
ORDER BY priority NULLS LAST
