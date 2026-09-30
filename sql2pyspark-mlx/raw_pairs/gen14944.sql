WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
)
SELECT val
FROM flattened
ORDER BY val DESC NULLS FIRST;
