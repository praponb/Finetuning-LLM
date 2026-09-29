WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        tier_prices,
        discounts,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
    WHERE plan NOT IN ('starter', 'pro')
)
-- Multi-level aggregation
SELECT
    CASE WHEN avg_tier_price >= 27 THEN 'PREMIUM' ELSE 'STANDARD' END AS avg_tier_price_tier,
    plan,
    COUNT(*) FILTER (WHERE plan = 'starter') AS starter_rows,
    SUM(CASE WHEN plan = 'starter' THEN avg_tier_price ELSE 0 END) AS starter_avg_tier_price,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    COUNT_IF(plan = 'pro') AS pro_events,
    COUNT(*) AS row_count,
    COUNT(*) AS event_count
FROM unpacked
GROUP BY CASE WHEN avg_tier_price >= 27 THEN 'PREMIUM' ELSE 'STANDARD' END, plan
HAVING COUNT(*) < 1000
