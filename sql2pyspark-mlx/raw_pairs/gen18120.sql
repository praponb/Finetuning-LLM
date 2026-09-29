WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
)
SELECT
    plan,
    effective_from,
    RANK() OVER (ORDER BY val NULLS FIRST) AS val_rank
FROM flattened;
