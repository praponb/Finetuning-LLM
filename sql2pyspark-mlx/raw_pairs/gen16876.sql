WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    CASE WHEN lowest_price >= 36 THEN 'LARGE' ELSE 'SMALL' END AS lowest_price_tier,
    MAX(entry_price) AS max_entry_price,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    SUM(CASE WHEN plan = 'starter' THEN lowest_price ELSE 0 END) AS starter_lowest_price,
    COUNT(*) AS num_rows
FROM unpacked
GROUP BY DATE_TRUNC('QUARTER', effective_from), CASE WHEN lowest_price >= 36 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY quarter_start NULLS LAST
