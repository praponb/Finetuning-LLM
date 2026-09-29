WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    effective_from,
    tier_value,
    DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('week', effective_from) ORDER BY tier_value) AS tier_value_dense_rank,
    RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_rank,
    RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_rank_2,
    COUNT(tier_value) OVER (PARTITION BY DATE_TRUNC('MONTH', effective_from)) AS effective_from_period_count_tier_value,
    RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_rank_3
FROM lines;
