WITH filtered AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATE_TRUNC('YEAR', hired_on) AS hired_on_year,
        DATEDIFF(DATE '2025-12-31', hired_on) AS days_since_hired_on,
        LENGTH(TRIM(team)) AS team_len
    FROM support_agents
),
latest_per_key AS (
    -- Keep the latest row per team, tier
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        hired_on_year,
        days_since_hired_on,
        team_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team, tier ORDER BY days_since_hired_on DESC, id DESC) AS recency_rank
        FROM filtered
    ) ranked
    WHERE recency_rank = 1
),
latest AS (
    -- Keep the latest row per team, tier
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        hired_on_year,
        days_since_hired_on,
        team_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY team, tier ORDER BY days_since_hired_on DESC, id DESC) AS row_num
        FROM latest_per_key
    ) ranked
    WHERE row_num = 1
)
-- Final projection
SELECT
    id,
    team,
    tier,
    hired_on_year,
    days_since_hired_on,
    team_len,
    CASE WHEN team IN ('VIP', 'Onboarding') THEN 'group_a' WHEN team = 'Tech' THEN 'group_b' ELSE 'other' END AS team_group,
    MONTH(hired_on_year) AS hired_on_year_month
FROM latest
WHERE tier NOT IN ('T3', 'T1')
ORDER BY hired_on_year, hired_on_year_month, id
LIMIT 20
