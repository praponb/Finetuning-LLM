SELECT
    id,
    tier_prices,
    discounts,
    AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
    SLICE(tier_limits, 1, 2) AS first_two_limits,
    ARRAY_MIN(tier_prices) AS lowest_price
FROM price_tiers;
