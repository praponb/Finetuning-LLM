SELECT actor AS name
FROM ticket_events
EXCEPT ALL
SELECT agent_name
FROM support_agents
ORDER BY name DESC
