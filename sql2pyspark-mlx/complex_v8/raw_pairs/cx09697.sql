WITH scoped AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        hired_on
    FROM support_agents
    WHERE team = 'VIP'
)
SELECT id
FROM scoped
WHERE hired_on < DATE '2024-12-31'
  AND team NOT IN ('Billing', 'Onboarding')
INTERSECT ALL
SELECT id
FROM support_agents
ORDER BY id DESC
