SELECT
    id,
    plan,
    currency,
    EXPLODE_OUTER(tier_limits) AS tier_value
FROM price_tiers;
