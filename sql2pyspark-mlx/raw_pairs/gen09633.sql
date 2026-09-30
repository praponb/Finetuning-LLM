SELECT
    id,
    plan,
    tier_limits,
    tier_prices,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
FROM price_tiers;
