SELECT csat, agent_id, opened_at
FROM tickets
WHERE channel = 'phone'
UNION
SELECT id AS csat, ticket_id AS agent_id, event_at AS opened_at
FROM ticket_events
ORDER BY agent_id DESC, opened_at
