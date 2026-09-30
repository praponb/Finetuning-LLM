WITH valid_agents AS (
    -- Filter support_agents
    SELECT *
    FROM support_agents
    WHERE team = 'Billing'
)
SELECT team, agent_name AS category, hired_on AS event_date
FROM valid_agents
WHERE hired_on IS NOT NULL
UNION ALL
SELECT tier AS team, agent_name AS category, hired_on AS event_date
FROM support_agents
WHERE UPPER(TRIM(agent_name)) LIKE 'H%'
  AND team NOT IN ('Billing', 'VIP')
UNION ALL
SELECT team, agent_name AS category, hired_on AS event_date
FROM support_agents
WHERE tier NOT IN ('T2', 'T1')
  AND team IS NOT NULL
ORDER BY event_date
