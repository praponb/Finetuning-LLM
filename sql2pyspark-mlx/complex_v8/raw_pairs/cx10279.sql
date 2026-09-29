SELECT
    'event' AS origin,
    id AS key_id,
    event_type AS category,
    event_at
FROM ticket_events
UNION ALL
SELECT
    'event_2' AS origin,
    id AS key_id,
    event_type AS category,
    event_at
FROM ticket_events
WHERE TO_DATE(event_at) < DATE '2025-02-24'
