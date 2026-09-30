SELECT
    id,
    effective_from,
    currency,
    tier_prices,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
    ARRAY_MIN(tier_prices) AS lowest_price,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    SLICE(tier_limits, 1, 2) AS first_two_limits
FROM price_tiers
WHERE currency IN ('USD', 'EUR', 'THB');
