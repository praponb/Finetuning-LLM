WITH scoped AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE team IN ('VIP', 'Onboarding')
      AND hired_on IS NOT NULL
)
SELECT tier
FROM scoped
INTERSECT
SELECT channel AS tier
FROM tickets
