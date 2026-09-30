WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        discounts,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('WEEK', effective_from) AS week_start,
    PERCENTILE_APPROX(entry_price, ARRAY(0.25, 0.5, 0.75)) AS entry_price_quartiles,
    PERCENTILE_APPROX(entry_price, ARRAY(0.25, 0.5, 0.75)) AS entry_price_quartiles_2,
    ROUND(SUM(entry_price) / NULLIF(COUNT(CASE WHEN plan = 'starter' THEN 1 END), 0), 2) AS entry_price_per_starter,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    ROUND(AVG(lowest_price), 2) AS avg_lowest_price,
    GROUPING_ID() AS grouping_id
FROM parsed
GROUP BY CUBE (DATE_TRUNC('WEEK', effective_from))
ORDER BY entry_price_per_starter DESC
