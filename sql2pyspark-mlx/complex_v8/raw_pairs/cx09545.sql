WITH prepared AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE tier NOT IN ('T3', 'T1')
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY team ORDER BY hired_on), 3) AS hired_on_cume_dist,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY MONTH(hired_on) ORDER BY hired_on DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on
    FROM prepared
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE hired_on_cume_dist < (SELECT MAX(hired_on_cume_dist) FROM ranked)
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM without_matches
    WHERE hired_on_cume_dist > (SELECT AVG(hired_on_cume_dist) FROM without_matches)
)
-- Subtotals with GROUPING SETS
SELECT
    team,
    tier,
    GROUPING(team) AS is_total_team,
    COUNT(*) AS row_count,
    MIN(hired_on) AS first_hired_on
FROM screened
GROUP BY GROUPING SETS ((team, tier), (team), (tier))
ORDER BY team NULLS LAST, tier NULLS LAST
