WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
)
SELECT
    effective_from,
    currency,
    element
FROM lines;
