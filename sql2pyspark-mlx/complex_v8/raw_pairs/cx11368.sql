WITH joined_ranges AS (
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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
annotated AS (
    -- Label and score rows
    SELECT *, CONCAT_WS('-', priority, event_type) AS priority_event_type_label
    FROM joined_ranges
    WHERE ticket_id >= 55
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    priority,
    agent_id,
    hours_since_open,
    priority_event_type_label,
    DATEDIFF(DATE '2026-01-01', event_at) AS days_since_event_at
FROM annotated
ORDER BY hours_since_open NULLS LAST
