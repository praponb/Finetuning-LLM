WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS tier_value
)
-- Ranking and gap features
SELECT
    effective_from,
    tier_value,
    PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
    CUME_DIST() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist
FROM lines
WHERE effective_from < '2024-12-15'
