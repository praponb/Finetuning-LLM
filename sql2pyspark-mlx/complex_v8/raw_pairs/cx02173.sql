SELECT ticket_id, event_at AS happened_at, id AS entity_id
FROM ticket_events
WHERE ticket_id IS NOT NULL
UNION ALL
SELECT ticket_id, event_at AS happened_at, id AS entity_id
FROM ticket_events
UNION ALL
SELECT id AS ticket_id, event_at AS happened_at, ticket_id AS entity_id
FROM ticket_events
ORDER BY ticket_id
