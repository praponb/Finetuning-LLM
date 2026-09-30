SELECT 'agent' AS origin, tier, hired_on AS day
FROM support_agents
WHERE hired_on >= DATE '2024-05-02'
  AND team IS NOT NULL
UNION
SELECT 'agent_2' AS origin, team, hired_on
FROM support_agents
WHERE team IN ('Tech', 'Onboarding')
ORDER BY day DESC, tier
