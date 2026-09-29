WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Aggregate with conditional metrics
SELECT
    currency,
    CASE WHEN lowest_price >= 27 THEN 'HIGH' ELSE 'LOW' END AS lowest_price_tier,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    AVG(lowest_price) AS avg_lowest_price,
    COUNT(*) AS row_count,
    GROUPING(currency) AS grouping_id
FROM unpacked
GROUP BY GROUPING SETS ((currency, CASE WHEN lowest_price >= 27 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', effective_from)), (currency, CASE WHEN lowest_price >= 27 THEN 'HIGH' ELSE 'LOW' END), (currency))
ORDER BY avg_lowest_price DESC
