WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
)
SELECT
    id,
    plan,
    tier_value,
    MAX(tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value,
    DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank
FROM lines;
