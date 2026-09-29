SELECT
    id,
    tier_limits,
    tier_prices,
    discounts,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
    ELEMENT_AT(tier_prices, 1) AS entry_price
FROM price_tiers;
