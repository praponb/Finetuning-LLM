SELECT
    'event' AS record_type,
    event_type AS category,
    event_at AS event_time,
    actor AS category_2
FROM ticket_events
WHERE event_type = 'reopened'
  AND TO_DATE(event_at) < DATE '2025-02-12'
UNION ALL
SELECT
    'event_2' AS record_type,
    event_type,
    event_at,
    actor
FROM ticket_events
UNION ALL
SELECT
    'ticket' AS record_type,
    priority,
    closed_at,
    channel
FROM tickets
