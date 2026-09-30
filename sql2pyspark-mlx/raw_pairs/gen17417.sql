SELECT
    id,
    plan,
    currency,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
FROM price_tiers
WHERE currency IN ('EUR', 'USD', 'THB');
