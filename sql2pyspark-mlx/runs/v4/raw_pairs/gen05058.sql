SELECT
    id,
    currency,
    tier_prices,
    discounts,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
FROM price_tiers
WHERE currency IN ('THB', 'USD');
