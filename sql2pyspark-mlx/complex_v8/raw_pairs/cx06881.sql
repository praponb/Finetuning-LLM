WITH valid_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        team,
        tier,
        hired_on,
        CONCAT(tier, '#', CAST(id AS STRING)) AS tier_ref
    FROM support_agents
    WHERE (agent_name IS NOT NULL OR hired_on >= DATE '2024-07-09')
)
SELECT tier_ref
FROM valid_agents
WHERE hired_on >= DATE '2024-07-28'
EXCEPT ALL
SELECT team AS tier_ref
FROM support_agents
WHERE hired_on IS NOT NULL
  AND team NOT IN ('Tech', 'Billing')
