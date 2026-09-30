SELECT
    id,
    effective_from,
    val
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val;
