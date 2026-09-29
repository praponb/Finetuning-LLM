WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        LOWER(REGEXP_REPLACE(TRIM(agent_name), ' ', '_')) AS agent_name_slug
    FROM support_agents
    WHERE team <> 'Onboarding'
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY team, tier ORDER BY hired_on DESC) AS hired_on_dense_rank,
        ROUND(CUME_DIST() OVER w_team_tier_by_hired_on_ties, 3) AS hired_on_cume_dist,
        LAST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY team ORDER BY hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on,
        LEAD(hired_on) OVER (PARTITION BY tier ORDER BY hired_on, id) AS next_hired_on,
        ROUND(CUME_DIST() OVER w_tier_by_hired_on, 3) AS hired_on_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY team ORDER BY hired_on ASC, id) AS hired_on_dense_rank_2,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY team ORDER BY hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on,
        NTILE(3) OVER (PARTITION BY team, tier ORDER BY hired_on DESC, id) AS hired_on_tercile,
        RANK() OVER w_tier_by_hired_on_ties AS hired_on_rank,
        LEAD(hired_on, 3) OVER w_team_tier_by_hired_on AS next3_hired_on,
        ROW_NUMBER() OVER (PARTITION BY tier ORDER BY hired_on, id) AS rn,
        LAST_VALUE(hired_on) OVER (PARTITION BY team ORDER BY hired_on ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_hired_on,
        DENSE_RANK() OVER (PARTITION BY tier ORDER BY hired_on) AS hired_on_dense_rank_3
    FROM valid
    WINDOW w_team_tier_by_hired_on_ties AS (PARTITION BY team, tier ORDER BY hired_on DESC), w_tier_by_hired_on AS (PARTITION BY tier ORDER BY hired_on, id), w_tier_by_hired_on_ties AS (PARTITION BY tier ORDER BY hired_on), w_team_tier_by_hired_on AS (PARTITION BY team, tier ORDER BY hired_on DESC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT *, LENGTH(TRIM(agent_name_slug)) AS agent_name_slug_len
    FROM windowed
    WHERE hired_on_dense_rank > 4
),
grouped AS (
    -- Group-level totals
    SELECT
        DATE_TRUNC('MONTH', last_known_hired_on) AS last_known_hired_on_month,
        SUM(agent_name_slug_len) AS total_agent_name_slug_len,
        ROUND(100 * COUNT(CASE WHEN team = 'VIP' THEN 1 END) / COUNT(*), 1) AS pct_team_vip
    FROM labeled
    WHERE tier IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', last_known_hired_on)
    HAVING AVG(hired_on_cume_dist_2) > 0.4
)
-- Final projection
SELECT DISTINCT pct_team_vip, total_agent_name_slug_len
FROM grouped
ORDER BY total_agent_name_slug_len DESC, pct_team_vip
