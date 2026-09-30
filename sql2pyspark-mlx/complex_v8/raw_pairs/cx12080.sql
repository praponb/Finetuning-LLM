WITH filtered_agents AS (
    -- Filter support_agents
    SELECT
        id,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE tier = 'T1'
      AND team = 'VIP'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        team,
        tier,
        hired_on,
        LAST_VALUE(hired_on) OVER (ORDER BY hired_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hired_on,
        LAG(hired_on) OVER (ORDER BY hired_on DESC NULLS FIRST, id) AS prev_hired_on,
        DATEDIFF(hired_on, LAG(hired_on) OVER (ORDER BY hired_on DESC NULLS FIRST, id)) AS days_since_prev_hired_on,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (ORDER BY hired_on DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on,
        ROW_NUMBER() OVER w_all_by_hired_on AS rn
    FROM filtered_agents
    WINDOW w_all_by_hired_on AS (ORDER BY hired_on DESC NULLS FIRST, id)
)
SELECT
    COALESCE(team, 'ALL') AS team,
    rn,
    COALESCE(tier, 'ALL') AS tier,
    GROUPING(team) AS is_total_team,
    MAX(days_since_prev_hired_on) AS max_days_since_prev_hired_on,
    COUNT(*) AS cnt
FROM with_trends
GROUP BY ROLLUP(team, rn, tier)
