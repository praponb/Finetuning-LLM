SELECT team, id AS entity_id
FROM support_agents
WHERE tier = 'T3'
UNION ALL
SELECT priority AS team, agent_id AS entity_id
FROM tickets
WHERE MONTH(opened_at) IN (1, 3)
ORDER BY team DESC
