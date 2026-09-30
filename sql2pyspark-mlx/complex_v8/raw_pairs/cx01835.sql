WITH valid AS (
    -- Scope the support_agents rows
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        TRUNC(hired_on, 'MM') AS hired_on_month_start
    FROM support_agents
    WHERE LENGTH(TRIM(agent_name)) >= 5
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY YEAR(hired_on), team ORDER BY hired_on_month_start DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on,
        ROUND(CUME_DIST() OVER (PARTITION BY YEAR(hired_on), team ORDER BY hired_on_month_start DESC), 3) AS hired_on_month_start_cume_dist,
        LAG(hired_on) OVER (PARTITION BY YEAR(hired_on), team ORDER BY hired_on_month_start DESC, id) AS prev_hired_on,
        DATEDIFF(hired_on, LAG(hired_on) OVER (PARTITION BY YEAR(hired_on), team ORDER BY hired_on_month_start DESC, id)) AS days_since_prev_hired_on,
        LAG(hired_on_month_start, 3) OVER w_hired_on_year_team_by_hired_3 AS prev3_hired_on_month_start,
        ROUND(PERCENT_RANK() OVER w_hired_on_year_team_by_hired_3, 3) AS hired_on_month_start_percent_rank
    FROM valid
    WINDOW w_hired_on_year_team_by_hired_3 AS (PARTITION BY YEAR(hired_on), team ORDER BY hired_on_month_start DESC, id)
)
SELECT
    COALESCE(tier, 'ALL') AS tier,
    COALESCE(team, 'ALL') AS team,
    GROUPING_ID() AS grouping_level,
    SUM(CASE WHEN tier IS NOT NULL THEN 1 ELSE 0 END) AS n_tier_is_not_null,
    COUNT(DISTINCT team) AS distinct_teams,
    COUNT(*) AS n_rows
FROM sequenced
GROUP BY ROLLUP(tier, team)
ORDER BY tier NULLS LAST, team NULLS LAST
