SELECT channel, priority, 'ticket' AS source
FROM tickets
WHERE MONTH(closed_at) IN (1, 3)
UNION ALL
SELECT agent_name AS channel, team AS priority, 'agent' AS source
FROM support_agents
WHERE tier IS NOT NULL
ORDER BY priority
