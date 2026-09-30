WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        DENSE_RANK() OVER w_part AS tier_value_dense_rank,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value
    FROM exploded
    WINDOW w_part AS (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(currency_max_tier_value, 2) AS currency_max_tier_value_rounded
    FROM enriched
    WHERE effective_from >= DATE '2024-06-06'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency_max_tier_value,
        currency_max_tier_value_rounded,
        DATE_TRUNC('WEEK', effective_from) AS effective_from_period,
        CEIL(currency_max_tier_value_rounded) AS currency_max_tier_value_rounded_whole
    FROM filtered
    WHERE effective_from > TO_DATE('2024-01-30')
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN currency_max_tier_value_rounded >= 80 THEN 'LARGE' ELSE 'SMALL' END AS currency_max_tier_value_rounded_tier,
        CASE WHEN currency_max_tier_value_rounded_whole >= 65 THEN 'LARGE' ELSE 'SMALL' END AS currency_max_tier_value_rounded_whole_tier,
        PERCENTILE_APPROX(currency_max_tier_value_rounded_whole, 0.5) AS currency_max_tier_value_rounded_whole_p50,
        COUNT(*) AS row_count,
        MIN(effective_from_period) AS first_effective_from_period
    FROM cleaned
    GROUP BY CASE WHEN currency_max_tier_value_rounded >= 80 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN currency_max_tier_value_rounded_whole >= 65 THEN 'LARGE' ELSE 'SMALL' END
    HAVING row_count > 1
)
SELECT
    currency_max_tier_value_rounded_tier,
    currency_max_tier_value_rounded_whole_tier,
    first_effective_from_period
FROM grouped
ORDER BY first_effective_from_period DESC, currency_max_tier_value_rounded_tier DESC;
