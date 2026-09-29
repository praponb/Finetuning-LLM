WITH valid AS (
    -- Scope the tickets rows
    SELECT
        id,
        agent_id,
        opened_at,
        channel,
        csat,
        tags,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM tickets
    WHERE channel IN ('phone', 'web')
      AND SIZE(tags) >= 1
),
enriched_agent AS (
    -- Join support_agents
    SELECT
        s.id,
        s.agent_id,
        s.opened_at,
        s.channel,
        s.csat,
        su.hired_on AS agent_hired_on,
        su.agent_name
    FROM valid s
    JOIN support_agents su ON su.id = s.agent_id
    WHERE LOWER(su.agent_name) LIKE '%to%'
),
with_parent AS (
    -- Attach agent attributes
    SELECT
        s.*,
        su.hired_on AS agent_hired_on_2,
        su.tier AS agent_tier,
        su.team AS agent_team
    FROM enriched_agent s
    LEFT JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with CUBE
SELECT
    agent_id,
    COALESCE(agent_tier, 'ALL') AS agent_tier,
    COALESCE(agent_team, 'ALL') AS agent_team,
    SUM(csat) AS total_csat,
    SUM(CASE WHEN agent_team <> 'Onboarding' THEN csat ELSE 0 END) AS csat_agent_team_onboarding
FROM with_parent
GROUP BY CUBE(agent_id, agent_tier, agent_team)
ORDER BY agent_id NULLS LAST, agent_tier NULLS LAST, agent_team NULLS LAST
