SELECT team AS label, id, hired_on
FROM support_agents
WHERE agent_name IS NOT NULL
UNION ALL
SELECT agent_name, id, hired_on
FROM support_agents
WHERE tier NOT IN ('T1', 'T2')
UNION ALL
SELECT tier, id, hired_on
FROM support_agents
