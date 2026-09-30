WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        effective_from,
        tier_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE plan IN ('enterprise', 'basic', 'starter')
)
-- Multi-level aggregation
SELECT
    plan,
    SUM(CASE WHEN plan = 'pro' THEN avg_tier_price ELSE 0 END) AS pro_avg_tier_price,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count,
    ROUND(SUM(lowest_price) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS lowest_price_per_starter
FROM flattened_attrs
GROUP BY plan;
