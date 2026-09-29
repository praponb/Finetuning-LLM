WITH cleaned AS (
    -- Filter tickets
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        channel,
        tags,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug
    FROM tickets
    WHERE requester_id BETWEEN 3 AND 20
      AND ARRAY_CONTAINS(tags, 'bug')
),
enriched AS (
    -- Attach agent attributes
    SELECT s.id, s.agent_id, su.team AS agent_team
    FROM cleaned s
    JOIN support_agents su ON su.id = s.agent_id
)
-- Subtotals with GROUPING SETS
SELECT
    agent_id,
    COALESCE(agent_team, 'ALL') AS agent_team,
    ARRAY_SORT(COLLECT_SET(agent_team)) AS agent_team_list,
    COUNT(*) AS n_rows,
    SUM(CASE WHEN agent_team = 'Onboarding' THEN 1 ELSE 0 END) AS n_agent_team_onboarding
FROM enriched
GROUP BY GROUPING SETS ((agent_id, agent_team), (agent_id))
ORDER BY agent_id NULLS LAST, agent_team NULLS LAST
