SELECT id AS ref_id, hired_on AS event_date, team AS name
FROM support_agents
UNION ALL
SELECT id, hired_on, tier
FROM support_agents
WHERE UPPER(TRIM(agent_name)) LIKE 'H%'
UNION ALL
SELECT id, hired_on, agent_name
FROM support_agents
WHERE tier IN ('T1', 'T3')
  AND UPPER(TRIM(agent_name)) LIKE 'I%'
ORDER BY event_date DESC
