SELECT
    id,
    plan,
    effective_from,
    currency,
    element
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(tier_prices) t AS element
