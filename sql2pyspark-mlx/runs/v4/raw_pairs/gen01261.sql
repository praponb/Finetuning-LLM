WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        currency,
        tier_value,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS FIRST) tier_value_dense_rank,
        MIN(tier_value) OVER (PARTITION BY currency) currency_min_tier_value,
        PERCENT_RANK() OVER (ORDER BY tier_value) tier_value_percent_rank,
        SUM(tier_value) OVER (PARTITION BY plan) plan_sum_tier_value,
        RANK() OVER (PARTITION BY plan, currency ORDER BY tier_value DESC NULLS FIRST) tier_value_rank
    FROM flattened
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, plan_sum_tier_value + tier_value plan_sum_tier_value_plus_tier_value
    FROM with_windows
    WHERE currency_min_tier_value < 30
)
-- Multi-level aggregation
SELECT
    CASE WHEN plan_sum_tier_value >= 402 THEN 'LARGE' ELSE 'SMALL' END AS plan_sum_tier_value_tier,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count,
    COUNT_IF(currency = 'THB') AS thb_events,
    ROUND(AVG(currency_min_tier_value), 2) AS avg_currency_min_tier_value,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list
FROM base
GROUP BY plan_sum_tier_value_tier
ORDER BY plan_sum_tier_value_tier DESC
