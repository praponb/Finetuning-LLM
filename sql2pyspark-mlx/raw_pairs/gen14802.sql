SELECT
    id,
    effective_from,
    currency,
    tier_limits,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    ELEMENT_AT(tier_prices, 1) AS entry_price
FROM price_tiers
WHERE effective_from BETWEEN '2023-09-21' AND '2024-09-01'
