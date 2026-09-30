WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        tier_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE plan IN ('starter', 'basic', 'pro')
)
-- Grouped metrics
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    plan,
    COUNT(*) FILTER (WHERE plan = 'basic') AS basic_rows,
    APPROX_PERCENTILE(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles,
    APPROX_PERCENTILE(avg_tier_price, 0.5) AS avg_tier_price_p50,
    PERCENTILE_APPROX(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles_2,
    COUNT_IF(plan = 'starter') AS starter_events
FROM parsed
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', effective_from), plan), (DATE_TRUNC('QUARTER', effective_from)))
ORDER BY avg_tier_price_p50 DESC
