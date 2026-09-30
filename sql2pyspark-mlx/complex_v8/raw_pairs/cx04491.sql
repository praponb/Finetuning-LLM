SELECT channel, agent_id, priority
FROM tickets
UNION ALL
SELECT event_type, ticket_id, actor
FROM ticket_events
ORDER BY channel, priority
