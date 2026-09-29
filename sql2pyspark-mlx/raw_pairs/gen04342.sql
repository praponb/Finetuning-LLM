WITH flattened AS (
    SELECT
        id,
        plan,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
filtered AS (
    SELECT
        *,
        CASE WHEN plan = 'basic' THEN 1 ELSE 0 END AS is_basic,
        CASE WHEN plan = 'basic' THEN 1 ELSE 0 END AS is_basic_2
    FROM flattened
    WHERE plan IN ('enterprise', 'basic', 'starter')
),
matched_keys AS (
    SELECT plan
    FROM filtered
    EXCEPT ALL
    SELECT plan
    FROM price_tiers
    WHERE plan = 'starter'
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN matched_keys k ON s.plan = k.plan
),
metrics AS (
    SELECT
        plan,
        is_basic_2,
        AVG(tier_value) OVER w_running AS plan_avg_tier_value,
        DENSE_RANK() OVER (ORDER BY is_basic_2 DESC) AS is_basic_2_dense_rank,
        STDDEV_SAMP(is_basic) OVER w_running AS plan_stddev_samp_is_basic
    FROM reconciled
    WINDOW w_running AS (PARTITION BY plan)
),
base AS (
    SELECT *, CEIL(plan_avg_tier_value) AS plan_avg_tier_value_whole
    FROM metrics
    WHERE is_basic_2_dense_rank < 4
),
scored AS (
    SELECT
        plan,
        is_basic_2,
        plan_avg_tier_value,
        is_basic_2_dense_rank,
        plan_avg_tier_value_whole,
        PERCENT_RANK() OVER (ORDER BY is_basic_2 DESC) AS is_basic_2_percent_rank,
        PERCENT_RANK() OVER w_recent AS is_basic_2_dense_rank_percent_rank
    FROM base
    WINDOW w_recent AS (PARTITION BY plan ORDER BY is_basic_2_dense_rank DESC NULLS FIRST)
)
SELECT
    CASE WHEN is_basic_2_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS is_basic_2_dense_rank_tier,
    plan,
    ROUND(AVG(is_basic_2), 2) AS avg_is_basic_2,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(*) AS num_rows,
    COUNT(CASE WHEN plan = 'enterprise' THEN 1 END) AS enterprise_count
FROM scored
GROUP BY GROUPING SETS ((CASE WHEN is_basic_2_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END, plan), (CASE WHEN is_basic_2_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END), ())
HAVING enterprise_count > 1
ORDER BY is_basic_2_dense_rank_tier DESC NULLS FIRST;
