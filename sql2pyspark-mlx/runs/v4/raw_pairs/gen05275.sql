SELECT
    id,
    plan,
    currency,
    tier_prices,
    discounts,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
    SLICE(tier_limits, 1, 2) AS first_two_limits,
    AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    ELEMENT_AT(tier_prices, 1) AS entry_price
FROM price_tiers
WHERE plan = 'starter'
