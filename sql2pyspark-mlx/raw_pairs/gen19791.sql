WITH flattened AS (
    SELECT
        id,
        plan,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
filtered AS (
    SELECT *, COALESCE(tier_value, 0) AS tier_value_filled
    FROM flattened
    WHERE tier_value IS NOT NULL
),
cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(plan)) AS plan_title,
        tier_value_filled + tier_value AS tier_value_filled_plus_tier_value
    FROM filtered
    WHERE plan NOT IN ('enterprise', 'starter', 'pro')
),
scored AS (
    SELECT
        id,
        plan,
        MIN(tier_value) OVER (PARTITION BY plan) AS plan_min_tier_value
    FROM cleaned
),
enriched AS (
    SELECT
        id,
        plan,
        SUM(plan_min_tier_value) OVER (PARTITION BY plan) AS plan_sum_plan_min_tier_value
    FROM scored
    WHERE plan_min_tier_value > 49
)
SELECT
    CASE WHEN plan_sum_plan_min_tier_value >= 359 THEN 'HIGH' ELSE 'LOW' END AS plan_sum_plan_min_tier_value_tier,
    plan,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    SUM(plan_sum_plan_min_tier_value) FILTER (WHERE plan = 'basic') AS basic_total_plan_sum_plan_min_tier_value
FROM enriched
GROUP BY CASE WHEN plan_sum_plan_min_tier_value >= 359 THEN 'HIGH' ELSE 'LOW' END, plan;
