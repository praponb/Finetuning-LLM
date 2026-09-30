WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(tier_value * 0.9, 2) AS adjusted_tier_value,
        DATE_FORMAT(effective_from, 'yyyy-MM-dd') AS effective_from_label
    FROM flattened
    WHERE effective_from > TO_DATE('2024-06-19')
      AND tier_value >= 75
),
scored AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (PARTITION BY plan ORDER BY adjusted_tier_value DESC NULLS LAST) AS adjusted_tier_value_rank
    FROM base
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT *, MAX(adjusted_tier_value) OVER (PARTITION BY plan) AS plan_max_adjusted_tier_value
    FROM scored
    WHERE tier_value >= 61
),
summary AS (
    -- Multi-level aggregation
    SELECT
        plan,
        CASE WHEN plan_max_adjusted_tier_value >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS plan_max_adjusted_tier_value_tier,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY SIZE(COLLECT_SET(plan)) DESC) AS num_plans_percent_rank
    FROM enriched
    GROUP BY plan, CASE WHEN plan_max_adjusted_tier_value >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    plan,
    plan_max_adjusted_tier_value_tier,
    num_plans,
    plan_list,
    num_plans_percent_rank
FROM summary
ORDER BY plan_max_adjusted_tier_value_tier DESC
