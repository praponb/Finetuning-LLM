SELECT
    id,
    effective_from,
    tier_limits,
    SLICE(tier_limits, 1, 2) AS first_two_limits,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
FROM price_tiers
WHERE MONTH(effective_from) IN (2, 7, 8);
