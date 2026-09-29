SELECT
    ticket_id AS qty,
    event_at,
    event_type AS category,
    'event' AS src
FROM ticket_events
UNION ALL
SELECT
    ticket_id,
    event_at,
    event_type,
    'event_2' AS src
FROM ticket_events
