WITH scoped AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        COALESCE(team, 'UNKNOWN') AS team_or_unknown,
        CASE WHEN team IN ('VIP', 'Onboarding') THEN 'group_a' WHEN team = 'Billing' THEN 'group_b' ELSE 'other' END AS team_group
    FROM support_agents
    WHERE tier = 'T1'
      AND team NOT IN ('Tech', 'VIP')
)
-- Final projection
SELECT
    id,
    team,
    tier,
    team_or_unknown,
    team_group,
    LENGTH(TRIM(team_or_unknown)) AS team_or_unknown_len
FROM scoped
ORDER BY team_or_unknown_len DESC, team_or_unknown DESC
