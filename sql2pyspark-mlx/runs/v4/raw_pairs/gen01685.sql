SELECT
    id,
    effective_from,
    element
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element
