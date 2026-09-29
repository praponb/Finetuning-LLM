SELECT ticket_id, event_type AS name
FROM ticket_events
WHERE actor IS NOT NULL
EXCEPT
SELECT id, team
FROM support_agents
WHERE agent_name IS NOT NULL
ORDER BY name DESC
