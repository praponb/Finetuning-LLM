SELECT
    id,
    plan,
    effective_from,
    currency,
    EXPLODE(tier_prices) AS val
FROM price_tiers
