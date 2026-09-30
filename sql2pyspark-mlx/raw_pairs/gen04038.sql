WITH unpacked AS (
    SELECT
        id,
        plan,
        tier_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    CASE WHEN lowest_price >= 32 THEN 'HIGH' ELSE 'LOW' END AS lowest_price_tier,
    COUNT(DISTINCT plan) AS distinct_plans,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    ROUND(SUM(entry_price) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS entry_price_per_basic
FROM unpacked
GROUP BY CASE WHEN lowest_price >= 32 THEN 'HIGH' ELSE 'LOW' END
HAVING COUNT(DISTINCT plan) > 1;
