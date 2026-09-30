WITH exploded AS (
    SELECT
        id,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT id, tier_value
FROM exploded
ORDER BY id NULLS LAST;
