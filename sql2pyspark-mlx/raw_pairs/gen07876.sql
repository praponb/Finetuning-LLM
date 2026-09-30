WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
)
SELECT
    id,
    plan,
    effective_from,
    tier_value
FROM lines
ORDER BY effective_from DESC NULLS LAST;
