WITH valid_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        team,
        tier,
        hired_on,
        LENGTH(TRIM(team)) AS team_len,
        LENGTH(TRIM(agent_name)) AS agent_name_len,
        LOWER(REGEXP_REPLACE(TRIM(team), ' ', '_')) AS team_slug
    FROM support_agents
    WHERE team = 'Tech'
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM valid_agents
    WHERE agent_name_len < (SELECT MAX(agent_name_len) FROM valid_agents)
)
-- Subtotals with ROLLUP
SELECT
    agent_name_len,
    team,
    ROUND(SUM(agent_name_len) / NULLIF(SUM(team_len), 0), 3) AS agent_name_len_to_team_len_ratio
FROM screened
GROUP BY ROLLUP(agent_name_len, team)
