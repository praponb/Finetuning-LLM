WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        tier_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    plan,
    SUM(CASE WHEN plan = 'basic' THEN entry_price ELSE 0 END) AS basic_entry_price,
    PERCENTILE_APPROX(entry_price, ARRAY(0.25, 0.5, 0.75)) AS entry_price_quartiles,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(*) AS cnt
FROM parsed
GROUP BY GROUPING SETS ((DATE_TRUNC('MONTH', effective_from), plan), (DATE_TRUNC('MONTH', effective_from)))
