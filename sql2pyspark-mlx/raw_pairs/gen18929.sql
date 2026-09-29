WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        currency,
        tier_prices,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE currency <> 'EUR'
)
-- Ranking and gap features
SELECT
    id,
    plan,
    currency,
    lowest_price,
    LAG(lowest_price, 1, 0) OVER (PARTITION BY plan ORDER BY lowest_price, id) AS prev_lowest_price,
    LAG(entry_price, 1) OVER (PARTITION BY plan, currency ORDER BY entry_price, id) AS prev_entry_price
FROM unpacked
WHERE entry_price <= 16;
