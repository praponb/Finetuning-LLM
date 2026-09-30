SELECT csat, channel
FROM tickets
WHERE priority NOT IN ('P2', 'P1')
  AND csat >= 3
INTERSECT
SELECT id, tier
FROM support_agents
WHERE LOWER(agent_name) LIKE '% n%'
ORDER BY csat, channel
