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
     AND e.event_at <= COALESCE(t.closed_at, t.opened_at + INTERVAL 3 DAYS)
),
exploded_detail AS (
    -- One row per event
    SELECT
        s.id,
        s.ticket_id,
        s.event_at,
        s.event_type,
        s.priority,
        s.agent_id,
        s.hours_since_open,
        t.id AS event_id,
        t.event_at AS event_event_at,
        t.event_type AS event_event_type,
        t.actor AS event_actor
    FROM covered s
    LEFT JOIN ticket_events t ON t.ticket_id = s.ticket_id AND t.actor = 'agent'
)
-- Final projection
SELECT
    id,
    ticket_id,
    event_type,
    priority,
    agent_id,
    event_id,
    event_event_at,
    event_event_type,
    TO_DATE(event_event_at) AS event_event_at_date
FROM exploded_detail
ORDER BY event_event_type DESC, id, event_id
LIMIT 20
