WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        tier_limits,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    id,
    entry_price,
    LAG(lowest_price) OVER (ORDER BY lowest_price, id) AS prev_lowest_price,
    LAG(entry_price) OVER (ORDER BY lowest_price, id) AS prev_entry_price,
    ROW_NUMBER() OVER (ORDER BY lowest_price, id) AS event_seq
FROM unpacked
