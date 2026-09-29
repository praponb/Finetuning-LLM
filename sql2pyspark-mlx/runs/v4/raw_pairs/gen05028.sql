SELECT
    id,
    plan,
    effective_from,
    currency,
    tier_limits,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    ARRAY_MIN(tier_prices) AS lowest_price,
    AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
FROM price_tiers
WHERE plan IN ('pro', 'starter', 'basic')
