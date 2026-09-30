SELECT
    id,
    plan,
    effective_from,
    currency,
    EXPLODE_OUTER(tier_limits) AS val
FROM price_tiers
