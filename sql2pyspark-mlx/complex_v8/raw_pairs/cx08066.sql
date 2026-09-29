WITH scoped_agents AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATEDIFF(DATE '2026-01-01', hired_on) AS days_since_hired_on
    FROM support_agents
    WHERE tier IN ('T3', 'T1')
),
tier_stats AS (
    -- Max days_since_hired_on per tier
    SELECT tier, MAX(days_since_hired_on) AS max_days_since_hired_on
    FROM scoped_agents
    GROUP BY tier
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_days_since_hired_on,
        ROUND(s.days_since_hired_on - g.max_days_since_hired_on, 2) AS days_since_hired_on_minus_max
    FROM scoped_agents s
    JOIN tier_stats g ON g.tier = s.tier
    WHERE s.days_since_hired_on = g.max_days_since_hired_on
)
SELECT
    team,
    tier,
    SUM(CASE WHEN tier <> 'T1' THEN 1 ELSE 0 END) AS n_tier_t1,
    SUM(days_since_hired_on_minus_max) AS total_days_since_hired_on_minus_max
FROM above_average
GROUP BY GROUPING SETS ((team, tier), (team), ())
