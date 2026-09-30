WITH valid AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATEDIFF(DATE '2025-12-31', hired_on) AS days_since_hired_on,
        DATE_FORMAT(hired_on, 'MMM yyyy') AS hired_on_label,
        DATEDIFF(DATE '2025-12-31', hired_on) AS days_since_hired_on_2
    FROM support_agents
    WHERE team = 'VIP'
      AND tier IN ('T3', 'T1')
),
team_stats AS (
    -- Avg days_since_hired_on_2 per team
    SELECT team, AVG(days_since_hired_on_2) AS avg_days_since_hired_on_2
    FROM valid
    GROUP BY team
),
outliers AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_days_since_hired_on_2,
        ROUND(s.days_since_hired_on_2 - g.avg_days_since_hired_on_2, 2) AS days_since_hired_on_2_minus_avg
    FROM valid s
    JOIN team_stats g ON g.team = s.team
    WHERE s.days_since_hired_on_2 > g.avg_days_since_hired_on_2
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        team,
        tier,
        hired_on,
        hired_on_label,
        days_since_hired_on_2,
        LAG(avg_days_since_hired_on_2) OVER w_team_by_days_since_hired_on AS prev_avg_days_since_hired_on_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY team ORDER BY days_since_hired_on_2), 3) AS days_since_hired_on_2_percent_rank,
        ROUND(days_since_hired_on - AVG(days_since_hired_on) OVER (PARTITION BY tier, team), 2) AS days_since_hired_on_vs_tier_team_avg,
        LAG(hired_on) OVER (PARTITION BY team, tier ORDER BY hired_on, id) AS prev_hired_on,
        COUNT(*) OVER w_tier_team AS rows_in_tier_team
    FROM outliers
    WINDOW w_team_by_days_since_hired_on AS (PARTITION BY team ORDER BY days_since_hired_on_2, id), w_tier_team AS (PARTITION BY tier, team)
),
summary AS (
    -- Summarise per group
    SELECT
        tier,
        team,
        DATE_TRUNC('MONTH', prev_hired_on) AS prev_hired_on_month,
        ARRAY_SORT(COLLECT_SET(tier)) AS tier_list,
        MIN(prev_hired_on) AS first_prev_hired_on,
        COUNT(*) AS row_count
    FROM sequenced
    GROUP BY tier, team, DATE_TRUNC('MONTH', prev_hired_on)
)
-- Final projection
SELECT tier, prev_hired_on_month, tier_list
FROM summary
WHERE prev_hired_on_month < TIMESTAMP '2025-02-05 09:00:00'
ORDER BY tier DESC
