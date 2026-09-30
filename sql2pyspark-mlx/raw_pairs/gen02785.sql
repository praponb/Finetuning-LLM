WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        currency,
        tier_value,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_percent_rank
    FROM exploded
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, tier_value_percent_rank * tier_value AS tier_value_percent_rank_times_tier_value
    FROM enriched
    WHERE tier_value BETWEEN 41 AND 54
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT *, CAST(tier_value_percent_rank AS INT) AS tier_value_percent_rank_int
    FROM cleaned
    WHERE tier_value_percent_rank <= 0.63
)
SELECT
    tier_value,
    tier_value_cume_dist,
    tier_value_percent_rank,
    tier_value_percent_rank_times_tier_value,
    tier_value_percent_rank_int
FROM cleaned_price_tiers
ORDER BY tier_value_percent_rank_int DESC;
