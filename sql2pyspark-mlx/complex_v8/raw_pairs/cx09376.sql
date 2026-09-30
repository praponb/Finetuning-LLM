WITH recent AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATE_FORMAT(hired_on, 'yyyy-MM-dd') AS hired_on_label,
        DATE_TRUNC('WEEK', hired_on) AS hired_on_week
    FROM support_agents
    WHERE tier = 'T2'
),
latest_per_key AS (
    -- Keep the latest row per tier, team
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        hired_on_label,
        hired_on_week
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY tier, team ORDER BY hired_on_week DESC, id DESC) AS row_num
        FROM recent
    ) ranked
    WHERE row_num = 1
)
-- Subtotals with CUBE
SELECT
    COALESCE(tier, 'ALL') AS tier,
    COALESCE(team, 'ALL') AS team,
    SUM(CASE WHEN team NOT IN ('Onboarding', 'Billing') THEN 1 ELSE 0 END) AS n_team_not_in_onboarding_billi,
    COUNT(*) AS n_rows
FROM latest_per_key
GROUP BY CUBE(tier, team)
ORDER BY tier NULLS LAST, team NULLS LAST
