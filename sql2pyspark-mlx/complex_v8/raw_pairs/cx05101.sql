WITH cleaned_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '% n%'
)
SELECT team
FROM cleaned_agents
WHERE tier IN ('T2', 'T1')
MINUS
SELECT team
FROM support_agents
WHERE tier = 'T2'
ORDER BY team
