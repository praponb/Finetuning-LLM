SELECT 'event' AS record_type, id, ticket_id
FROM ticket_events
UNION ALL
SELECT 'event_2' AS record_type, id, ticket_id
FROM ticket_events
ORDER BY record_type, ticket_id
