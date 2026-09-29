WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, FLOOR(tier_value) tier_value_whole
    FROM lines
    WHERE tier_value >= 31
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, tier_value + tier_value_whole tier_value_plus_tier_value_whole
    FROM filtered
    WHERE tier_value_whole > 24
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(tier_value, 0) tier_value_rounded,
        tier_value_whole * tier_value tier_value_whole_times_tier_value
    FROM base
    WHERE tier_value_whole BETWEEN 24 AND 67
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, PERCENT_RANK() OVER (ORDER BY tier_value_whole_times_tier_value NULLS FIRST) tier_value_whole_times_tier_value_percent_rank
    FROM valid
),
base_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        tier_value,
        tier_value_plus_tier_value_whole,
        tier_value_whole_times_tier_value,
        tier_value_rounded - tier_value_whole tier_value_rounded_minus_tier_value_whole
    FROM scored
    WHERE tier_value_rounded IS NOT NULL
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value_plus_tier_value_whole >= 84 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_plus_tier_value_whole_tier,
    CASE WHEN tier_value_whole_times_tier_value >= 85 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_whole_times_tier_value_tier,
    AVG(tier_value) AS avg_tier_value,
    PERCENTILE_APPROX(tier_value_whole_times_tier_value, 0.95) AS tier_value_whole_times_tier_value_p95,
    PERCENTILE_APPROX(tier_value, 0.9) AS tier_value_p90,
    GROUPING_ID() AS grouping_level
FROM base_price_tiers
GROUP BY GROUPING SETS ((CASE WHEN tier_value_plus_tier_value_whole >= 84 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN tier_value_whole_times_tier_value >= 85 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN tier_value_plus_tier_value_whole >= 84 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY tier_value_p90
