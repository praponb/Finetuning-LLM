SELECT
    id,
    plan,
    effective_from,
    currency,
    EXPLODE(tier_limits) AS val
FROM price_tiers
