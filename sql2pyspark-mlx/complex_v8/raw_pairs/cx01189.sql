SELECT id AS entity_id, event_at, 'event' AS source
FROM ticket_events
WHERE actor = 'customer'
UNION ALL
SELECT ticket_id AS entity_id, event_at, 'event_2' AS source
FROM ticket_events
ORDER BY entity_id DESC
