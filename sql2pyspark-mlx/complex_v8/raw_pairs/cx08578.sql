WITH valid_agents AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        DATE_TRUNC('WEEK', hired_on) AS hired_on_week,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
    WHERE tier IN ('T1', 'T2')
),
grouped AS (
    -- Group-level totals
    SELECT
        YEAR(hired_on) AS hired_on_year,
        team,
        COUNT(DISTINCT tier) AS distinct_tiers,
        ROUND(100 * COUNT(CASE WHEN tier IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_tier_is_not_null,
        COUNT(*) FILTER (WHERE LENGTH(TRIM(agent_name)) >= 4) AS n_length_trim_agent_name_4
    FROM valid_agents
    GROUP BY 1, 2
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        hired_on_year,
        team,
        pct_tier_is_not_null,
        LEAD(n_length_trim_agent_name_4, 2) OVER (ORDER BY pct_tier_is_not_null DESC, hired_on_year, team) AS next2_n_length_trim_agent_name_4,
        NTILE(5) OVER w_all_by_pct_tier_is_not_null AS pct_tier_is_not_null_quintile,
        RANK() OVER w_all_by_pct_tier_is_not_null AS pct_tier_is_not_null_rank,
        SUM(pct_tier_is_not_null) OVER (ORDER BY pct_tier_is_not_null DESC, hired_on_year, team ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pct_tier_is_not_null
    FROM grouped
    WINDOW w_all_by_pct_tier_is_not_null AS (ORDER BY pct_tier_is_not_null DESC, hired_on_year, team)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        hired_on_year,
        team,
        pct_tier_is_not_null_quintile,
        running_pct_tier_is_not_null,
        ROUND(running_pct_tier_is_not_null - AVG(running_pct_tier_is_not_null) OVER w_next2_n_length_trim_agent_na, 2) AS running_pct_tier_is_not_null_vs_next2_n_length_t,
        LAST_VALUE(pct_tier_is_not_null_rank) OVER (PARTITION BY next2_n_length_trim_agent_name_4 ORDER BY next2_n_length_trim_agent_name_4 DESC, hired_on_year, team ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_pct_tier_is_not_null_rank,
        ROUND(pct_tier_is_not_null_rank - AVG(pct_tier_is_not_null_rank) OVER w_next2_n_length_trim_agent_na, 2) AS pct_tier_is_not_null_rank_vs_next2_n_length_trim,
        ROUND(hired_on_year / NULLIF(SUM(hired_on_year) OVER (PARTITION BY next2_n_length_trim_agent_name_4, team), 0), 4) AS hired_on_year_share_of_next2_n_length_trim_agent,
        pct_tier_is_not_null - LAG(pct_tier_is_not_null) OVER (ORDER BY pct_tier_is_not_null DESC, hired_on_year, team) AS pct_tier_is_not_null_change,
        DENSE_RANK() OVER w_next2_n_length_trim_agent_na_3 AS next2_n_length_trim_agent_name_4_dense_rank
    FROM windowed
    WINDOW w_next2_n_length_trim_agent_na AS (PARTITION BY next2_n_length_trim_agent_name_4, team), w_next2_n_length_trim_agent_na_3 AS (PARTITION BY next2_n_length_trim_agent_name_4 ORDER BY next2_n_length_trim_agent_name_4 DESC, hired_on_year, team)
)
SELECT hired_on_year, team, metric, metric_value
FROM (
    SELECT hired_on_year, team, pct_tier_is_not_null_quintile, last_pct_tier_is_not_null_rank
    FROM with_trends
) src
UNPIVOT (
    metric_value FOR metric IN (pct_tier_is_not_null_quintile AS m_pct_tier_is_not_null_quintile, last_pct_tier_is_not_null_rank AS last_pct_tier_is_not_null_rank_v)
) unp
