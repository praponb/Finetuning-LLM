SELECT
    id,
    plan,
    val
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(tier_limits) li AS val;
