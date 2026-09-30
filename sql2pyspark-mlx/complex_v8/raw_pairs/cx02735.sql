SELECT id, hired_on AS event_date, agent_name AS name
FROM support_agents
WHERE team IS NOT NULL
  AND agent_name IS NOT NULL
UNION ALL
SELECT id, hired_on, agent_name
FROM support_agents
WHERE YEAR(hired_on) = 2025
