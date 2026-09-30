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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 2 DAYS)
),
line_level_events AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS event_id,
        t.event_type AS event_event_type,
        t.event_at AS event_event_at,
        t.actor AS event_actor
    FROM matched s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id
)
SELECT
    id,
    priority,
    hours_since_open,
    event_id,
    event_event_type,
    event_event_at,
    COALESCE(priority, 'UNKNOWN') AS priority_or_unknown
FROM line_level_events
ORDER BY id NULLS LAST, event_event_type NULLS LAST, event_id
LIMIT 10
