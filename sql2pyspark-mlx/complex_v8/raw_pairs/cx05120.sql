SELECT channel AS label
FROM tickets
WHERE MONTH(opened_at) IN (1, 3)
EXCEPT
SELECT agent_name AS label
FROM support_agents
ORDER BY label
