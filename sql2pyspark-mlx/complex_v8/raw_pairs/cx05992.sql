WITH filtered_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
)
SELECT tier, id AS qty, hired_on AS day
FROM filtered_agents
WHERE LENGTH(TRIM(agent_name)) >= 7
UNION
SELECT team, id, hired_on
FROM support_agents
ORDER BY qty, day
