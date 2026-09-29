SELECT
    id,
    plan,
    currency,
    tier_limits,
    discounts,
    ARRAY_MIN(tier_prices) AS lowest_price,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
FROM price_tiers
WHERE currency = 'USD';
