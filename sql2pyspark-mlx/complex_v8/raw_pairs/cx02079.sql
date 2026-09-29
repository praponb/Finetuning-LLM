WITH prepared_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CONCAT_WS(' / ', tier, team) AS tier_team_label
    FROM support_agents
    WHERE UPPER(TRIM(agent_name)) LIKE 'K%'
      AND team IN ('VIP', 'Billing')
),
unioned AS (
    -- Stack the branches
    SELECT tier, hired_on, 'agent' AS source
    FROM prepared_agents
    UNION
    SELECT team, hired_on, 'agent_2' AS source
    FROM support_agents
    WHERE team IN ('Tech', 'Onboarding')
)
SELECT source, COUNT(*) AS n_rows, MAX(hired_on) AS latest_hired_on
FROM unioned
GROUP BY source
ORDER BY source
