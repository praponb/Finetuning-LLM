SELECT id AS key_id, ticket_id, 'event' AS src
FROM ticket_events
WHERE event_type IN ('resolved', 'replied')
  AND ticket_id IS NOT NULL
UNION ALL
SELECT ticket_id, id, 'event_2' AS src
FROM ticket_events
