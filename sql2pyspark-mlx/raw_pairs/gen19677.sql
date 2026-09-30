SELECT
    id,
    effective_from,
    currency,
    val
FROM price_tiers
LATERAL VIEW EXPLODE(discounts) x AS val;
