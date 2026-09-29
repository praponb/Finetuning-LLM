WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(currency) AS currency_lower
    FROM lines
    WHERE currency = 'USD'
),
scored AS (
    -- Ranking and gap features
    SELECT
        tier_value,
        currency_lower,
        COUNT(tier_value) OVER (PARTITION BY currency) AS currency_count_tier_value,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        AVG(tier_value) OVER (PARTITION BY currency) AS currency_avg_tier_value,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS FIRST) AS tier_value_rank
    FROM filtered
),
filtered_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        currency_lower,
        currency_count_tier_value,
        currency_min_tier_value,
        currency_avg_tier_value,
        tier_value_rank,
        LEAST(currency_count_tier_value, currency_min_tier_value) AS least_currency_count_tier_value_currency_min_tier_value
    FROM scored
    WHERE currency_min_tier_value BETWEEN 26 AND 71
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        currency_count_tier_value,
        least_currency_count_tier_value_currency_min_tier_value,
        RANK() OVER (ORDER BY currency_avg_tier_value DESC NULLS LAST) AS currency_avg_tier_value_rank,
        CUME_DIST() OVER (ORDER BY currency_min_tier_value DESC NULLS FIRST) AS currency_min_tier_value_cume_dist
    FROM filtered_price_tiers
)
SELECT currency_min_tier_value_cume_dist
FROM scored_price_tiers
ORDER BY currency_min_tier_value_cume_dist NULLS FIRST
