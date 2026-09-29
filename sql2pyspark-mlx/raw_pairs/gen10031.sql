WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
)
SELECT
    plan,
    effective_from,
    MAX(tier_value) OVER (PARTITION BY DATE_TRUNC('MONTH', effective_from), plan) effective_from_period_max_tier_value,
    DENSE_RANK() OVER (ORDER BY tier_value NULLS FIRST) tier_value_dense_rank
FROM exploded;
