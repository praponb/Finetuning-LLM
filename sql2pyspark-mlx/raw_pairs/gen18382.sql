WITH unpacked AS (
    SELECT
        id,
        plan,
        tier_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
SELECT
    plan,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    APPROX_PERCENTILE(entry_price, 0.5) AS entry_price_p50,
    ROUND(SUM(entry_price) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS entry_price_per_basic,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list_2,
    ROUND(SUM(entry_price) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS entry_price_per_basic_2
FROM unpacked
GROUP BY plan
