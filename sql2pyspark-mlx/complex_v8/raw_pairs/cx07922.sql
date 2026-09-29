SELECT ticket_id, id AS key_id
FROM ticket_events
WHERE event_type = 'replied'
UNION ALL
SELECT id, ticket_id
FROM ticket_events
WHERE event_type IN ('escalated', 'reopened', 'replied')
