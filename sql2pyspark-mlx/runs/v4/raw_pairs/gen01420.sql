SELECT
    id,
    plan,
    effective_from,
    element
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(discounts) e AS element
