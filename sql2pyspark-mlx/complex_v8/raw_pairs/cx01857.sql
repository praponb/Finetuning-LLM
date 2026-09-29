WITH valid_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on
    FROM support_agents
    WHERE hired_on >= DATE '2024-07-09'
      AND tier NOT IN ('T2', 'T1')
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        agent_name,
        team,
        hired_on,
        DATE_TRUNC('WEEK', hired_on) AS hired_on_week
    FROM valid_agents
)
SELECT
    id,
    agent_name,
    team,
    hired_on,
    hired_on_week,
    MONTH(hired_on_week) AS hired_on_week_month,
    SUBSTRING(TRIM(team), 1, 2) AS team_prefix
FROM scored
ORDER BY team
