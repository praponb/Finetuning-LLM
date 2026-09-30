WITH valid_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        QUARTER(hired_on) AS hired_on_quarter
    FROM support_agents
    WHERE LOWER(agent_name) LIKE '%a %'
      AND tier = 'T2'
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(hired_on_quarter / NULLIF(SUM(hired_on_quarter) OVER (PARTITION BY tier), 0), 4) AS hired_on_quarter_share_of_tier,
        MAX(hired_on_quarter) OVER (PARTITION BY DATE_TRUNC('MONTH', hired_on), team) AS hired_on_month_team_max_hired_on_quarter,
        SUM(hired_on_quarter) OVER (PARTITION BY DATE_TRUNC('MONTH', hired_on), team) AS hired_on_month_team_sum_hired_on_quarter,
        LAST_VALUE(hired_on_quarter) OVER (PARTITION BY hired_on_quarter ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hired_on_quarter,
        ROUND(hired_on_quarter - AVG(hired_on_quarter) OVER (PARTITION BY YEAR(hired_on)), 2) AS hired_on_quarter_vs_hired_on_year_avg,
        NTILE(5) OVER (PARTITION BY tier, hired_on_quarter ORDER BY hired_on DESC, id) AS hired_on_quintile,
        ROUND(hired_on_quarter / NULLIF(SUM(hired_on_quarter) OVER w_hired_on_year, 0), 4) AS hired_on_quarter_share_of_hired_on_year,
        ROW_NUMBER() OVER (ORDER BY hired_on, id) AS hired_on_seq,
        MIN(hired_on_quarter) OVER (PARTITION BY DATE_TRUNC('MONTH', hired_on), team) AS hired_on_month_team_min_hired_on_quarter,
        SUM(hired_on_quarter) OVER (PARTITION BY YEAR(hired_on)) AS hired_on_year_sum_hired_on_quarter
    FROM valid_agents
    WINDOW w_hired_on_year AS (PARTITION BY YEAR(hired_on))
)
SELECT
    id,
    agent_name,
    team,
    tier,
    hired_on,
    hired_on_quarter,
    hired_on_month_team_max_hired_on_quarter,
    hired_on_month_team_sum_hired_on_quarter,
    last_hired_on_quarter,
    hired_on_quarter_vs_hired_on_year_avg,
    hired_on_quintile,
    hired_on_seq,
    hired_on_month_team_min_hired_on_quarter,
    hired_on_year_sum_hired_on_quarter,
    COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
FROM with_trends
ORDER BY hired_on_quintile, id
LIMIT 20
