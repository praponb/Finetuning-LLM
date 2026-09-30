SELECT
    tier,
    agent_name AS name,
    hired_on,
    'agent' AS source
FROM support_agents
UNION ALL
SELECT
    team,
    tier,
    hired_on,
    'agent_2' AS source
FROM support_agents
