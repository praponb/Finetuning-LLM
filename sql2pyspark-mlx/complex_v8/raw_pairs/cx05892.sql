WITH filtered_agents AS (
    -- Filter support_agents
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on
    FROM support_agents
    WHERE UPPER(TRIM(agent_name)) LIKE 'F%'
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(hired_on) AS hired_on_year,
        tier,
        SUM(CASE WHEN tier = 'T2' THEN 1 ELSE 0 END) AS n_tier_t2,
        ARRAY_SORT(COLLECT_SET(agent_name)) AS agent_name_list,
        COUNT(*) AS row_count,
        SUM(CASE WHEN UPPER(TRIM(agent_name)) LIKE 'L%' THEN 1 ELSE 0 END) AS n_upper_trim_agent_name_like_l
    FROM filtered_agents
    GROUP BY YEAR(hired_on), tier
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        hired_on_year,
        tier,
        n_tier_t2,
        agent_name_list,
        MAX(row_count) OVER (ORDER BY n_tier_t2, hired_on_year, tier ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS row_count_max_3,
        ROUND(CUME_DIST() OVER (PARTITION BY n_upper_trim_agent_name_like_l, n_tier_t2 ORDER BY row_count ASC), 3) AS row_count_cume_dist,
        row_count - LAG(row_count) OVER (PARTITION BY n_upper_trim_agent_name_like_l, n_tier_t2 ORDER BY row_count ASC, hired_on_year, tier) AS row_count_change,
        MAX(row_count) OVER (PARTITION BY tier) AS tier_max_row_count,
        LAST_VALUE(row_count) OVER (ORDER BY n_tier_t2, hired_on_year, tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_row_count,
        SUM(hired_on_year) OVER (PARTITION BY tier) AS tier_sum_hired_on_year
    FROM totals
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked
    WHERE tier_sum_hired_on_year < (SELECT MAX(tier_sum_hired_on_year) FROM ranked)
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(last_row_count / NULLIF(SUM(last_row_count) OVER w_last_row_count, 0), 4) AS last_row_count_share_of_last_row_count,
        FIRST_VALUE(last_row_count) OVER (ORDER BY last_row_count DESC NULLS FIRST, hired_on_year, tier ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_last_row_count,
        ROUND(CUME_DIST() OVER (ORDER BY last_row_count DESC NULLS FIRST), 3) AS last_row_count_cume_dist,
        ROUND(AVG(row_count_max_3) OVER (ORDER BY last_row_count DESC NULLS FIRST, hired_on_year, tier ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS row_count_max_3_avg_6_prior,
        LEAD(row_count_cume_dist) OVER w_all_by_last_row_count AS next_row_count_cume_dist
    FROM with_activity
    WINDOW w_last_row_count AS (PARTITION BY last_row_count), w_all_by_last_row_count AS (ORDER BY last_row_count DESC NULLS FIRST, hired_on_year, tier)
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM ranked_2
    WHERE n_tier_t2 < (SELECT MAX(n_tier_t2) FROM ranked_2)
)
-- Final projection
SELECT
    hired_on_year,
    tier,
    n_tier_t2,
    agent_name_list,
    row_count_max_3,
    row_count_cume_dist,
    row_count_change,
    tier_max_row_count,
    last_row_count,
    tier_sum_hired_on_year,
    first_last_row_count,
    last_row_count_cume_dist,
    row_count_max_3_avg_6_prior,
    next_row_count_cume_dist,
    ABS(last_row_count_cume_dist - 0.5) AS last_row_count_cume_dist_distance,
    row_count_change + next_row_count_cume_dist AS row_count_change_plus_next_row_count_cume_dist
FROM without_matches
ORDER BY tier, hired_on_year
LIMIT 10
