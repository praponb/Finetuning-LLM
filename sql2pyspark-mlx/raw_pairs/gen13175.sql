SELECT
    id,
    plan,
    currency,
    tier_limits,
    tier_prices,
    discounts,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    SLICE(tier_limits, 1, 2) AS first_two_limits
FROM price_tiers
