SELECT
    id,
    plan,
    effective_from,
    currency,
    EXPLODE(discounts) AS element
FROM price_tiers
