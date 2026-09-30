SELECT
    id,
    plan,
    val
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(tier_prices) x AS val
