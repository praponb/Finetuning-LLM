SELECT id AS key_id
FROM support_agents
WHERE LENGTH(TRIM(agent_name)) >= 5
EXCEPT ALL
SELECT id AS key_id
FROM tickets
