WITH scoped AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CASE WHEN tier IN ('T1', 'T3') THEN 'group_a' WHEN tier = 'T2' THEN 'group_b' ELSE 'other' END AS tier_group
    FROM support_agents
    WHERE team NOT IN ('Onboarding', 'VIP')
)
SELECT id AS ref_id
FROM scoped
WHERE tier_group IN ('other', 'group_b')
EXCEPT
SELECT id
FROM support_agents
ORDER BY ref_id
