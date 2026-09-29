SELECT
    'event' AS origin,
    actor,
    event_at,
    event_type
FROM ticket_events
WHERE TO_DATE(event_at) < DATE '2025-01-31'
  AND ticket_id >= 46
UNION
SELECT
    'event_2' AS origin,
    actor,
    event_at,
    event_type
FROM ticket_events
