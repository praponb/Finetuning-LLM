SELECT
    id,
    plan,
    effective_from,
    currency,
    tier_limits,
    tier_prices,
    discounts,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    ARRAY_MIN(tier_prices) AS lowest_price,
    AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
FROM price_tiers
