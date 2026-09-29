WITH valid AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        tier,
        hired_on,
        DATE_TRUNC('WEEK', hired_on) AS hired_on_week
    FROM support_agents
)
SELECT
    tier,
    id,
    hired_on,
    'agent' AS src
FROM valid
WHERE UPPER(TRIM(agent_name)) LIKE 'J%'
  AND tier NOT IN ('T3', 'T1')
UNION ALL
SELECT
    agent_name AS tier,
    id,
    hired_on,
    'agent_2' AS src
FROM support_agents
