SELECT
    id,
    currency,
    element
FROM price_tiers
LATERAL VIEW EXPLODE(discounts) x AS element
