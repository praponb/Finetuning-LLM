SELECT event_type AS category
FROM ticket_events
EXCEPT ALL
SELECT agent_name
FROM support_agents
WHERE team IN ('Onboarding', 'Tech')
ORDER BY category DESC
