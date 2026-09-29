SELECT id AS qty, team AS category
FROM support_agents
EXCEPT
SELECT ticket_id AS qty, actor AS category
FROM ticket_events
ORDER BY qty DESC
