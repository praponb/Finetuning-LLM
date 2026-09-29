WITH valid AS (
    -- Scope the support_agents rows
    SELECT
        id,
        team,
        tier,
        hired_on,
        UPPER(TRIM(agent_name)) AS agent_name_clean
    FROM support_agents
    WHERE team = 'Tech'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        RANK() OVER (PARTITION BY tier, team ORDER BY hired_on DESC) AS hired_on_rank,
        DATEDIFF(hired_on, LAG(hired_on) OVER (PARTITION BY tier, team ORDER BY hired_on DESC, id)) AS days_since_prev_hired_on,
        LEAD(hired_on, 2) OVER (ORDER BY hired_on, id) AS next2_hired_on,
        ROUND(CUME_DIST() OVER (ORDER BY hired_on), 3) AS hired_on_cume_dist
    FROM valid
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        LOWER(TRIM(tier)) AS tier_clean,
        LENGTH(TRIM(agent_name_clean)) AS agent_name_clean_len
    FROM with_windows
),
labeled AS (
    -- Add calculated columns
    SELECT
        id,
        team,
        tier,
        hired_on,
        agent_name_clean,
        days_since_prev_hired_on,
        next2_hired_on,
        hired_on_cume_dist,
        tier_clean,
        ROUND(hired_on_cume_dist * 1.5, 2) AS hired_on_cume_dist_adj,
        QUARTER(hired_on) AS hired_on_quarter
    FROM tagged
    WHERE agent_name_clean IS NOT NULL
)
-- Subtotals with GROUPING SETS
SELECT
    team,
    hired_on_quarter,
    GROUPING_ID() AS gid,
    MIN(hired_on_cume_dist_adj) AS min_hired_on_cume_dist_adj,
    COUNT(*) AS cnt,
    ROUND(SUM(hired_on_quarter) / NULLIF(SUM(hired_on_cume_dist_adj), 0), 3) AS hired_on_quarter_to_hired_on_cume_dist_adj_ratio
FROM labeled
GROUP BY GROUPING SETS ((team, hired_on_quarter), (team), (hired_on_quarter))
ORDER BY team NULLS LAST, hired_on_quarter NULLS LAST
