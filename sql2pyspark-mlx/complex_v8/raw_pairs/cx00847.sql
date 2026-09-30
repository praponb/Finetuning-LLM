WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_name,
        team,
        tier,
        hired_on,
        CASE WHEN tier IN ('T1', 'T3') THEN 'group_a' WHEN tier = 'T2' THEN 'group_b' ELSE 'other' END AS tier_group,
        COALESCE(tier, 'UNKNOWN') AS tier_or_unknown
    FROM support_agents
    WHERE hired_on < DATE '2025-02-26'
      AND LOWER(agent_name) LIKE '%ri%'
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER w_team_by_hired_on AS hired_on_quartile,
        FIRST_VALUE(hired_on) IGNORE NULLS OVER (PARTITION BY team ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_hired_on,
        NTILE(5) OVER (PARTITION BY team ORDER BY hired_on, id) AS hired_on_quintile,
        DATEDIFF(hired_on, LAG(hired_on) OVER (ORDER BY hired_on ASC, id)) AS days_since_prev_hired_on,
        ROUND(PERCENT_RANK() OVER w_all_by_hired_on, 3) AS hired_on_percent_rank,
        NTH_VALUE(hired_on, 2) OVER (PARTITION BY team ORDER BY hired_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_hired_on
    FROM prepared
    WINDOW w_team_by_hired_on AS (PARTITION BY team ORDER BY hired_on, id), w_all_by_hired_on AS (ORDER BY hired_on ASC, id)
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_FORMAT(hired_on, 'yyyy-MM') AS hired_on_ym,
        tier_group,
        hired_on_quintile,
        SUM(CASE WHEN tier IS NOT NULL THEN hired_on_quartile ELSE 0 END) AS hired_on_quartile_tier_is_not_null,
        PERCENTILE_APPROX(hired_on_quartile, 0.5) AS median_hired_on_quartile,
        ROUND(STDDEV(hired_on_quartile), 2) AS stddev_hired_on_quartile,
        ROUND(STDDEV(hired_on_quintile), 2) AS stddev_hired_on_quintile
    FROM metrics
    GROUP BY 1, 2, 3
    HAVING SUM(days_since_prev_hired_on) > 50
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        hired_on_ym,
        tier_group,
        hired_on_quintile,
        stddev_hired_on_quartile,
        stddev_hired_on_quintile,
        LAST_VALUE(hired_on_quintile) IGNORE NULLS OVER (PARTITION BY tier_group, hired_on_quintile ORDER BY median_hired_on_quartile NULLS LAST, hired_on_ym, tier_group, hired_on_quintile ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_hired_on_quintile,
        COUNT(*) OVER (PARTITION BY hired_on_quintile) AS rows_in_hired_on_quintile,
        MIN(stddev_hired_on_quartile) OVER w_hired_on_quintile AS hired_on_quintile_min_stddev_hired_on_quartile,
        FIRST_VALUE(hired_on_quintile) OVER (ORDER BY hired_on_quartile_tier_is_not_null ASC, hired_on_ym, tier_group, hired_on_quintile ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_hired_on_quintile,
        MAX(median_hired_on_quartile) OVER (PARTITION BY tier_group, hired_on_quintile ORDER BY median_hired_on_quartile NULLS LAST, hired_on_ym, tier_group, hired_on_quintile ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS median_hired_on_quartile_max_6_prior,
        SUM(median_hired_on_quartile) OVER (ORDER BY hired_on_quartile_tier_is_not_null ASC, hired_on_ym, tier_group, hired_on_quintile ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS median_hired_on_quartile_sum_2
    FROM grouped
    WINDOW w_hired_on_quintile AS (PARTITION BY hired_on_quintile)
)
SELECT hired_on_ym, tier_group, metric_name, metric_value
FROM (
    SELECT hired_on_ym, tier_group, first_hired_on_quintile, median_hired_on_quartile_max_6_prior, median_hired_on_quartile_sum_2
    FROM ranked
) src
UNPIVOT (
    metric_value FOR metric_name IN (first_hired_on_quintile, median_hired_on_quartile_max_6_prior, median_hired_on_quartile_sum_2)
) unp
ORDER BY hired_on_ym, tier_group, metric_name
