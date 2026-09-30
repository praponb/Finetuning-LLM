SELECT
    id,
    plan,
    effective_from,
    EXPLODE(tier_limits) AS val
FROM price_tiers
