WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist
    FROM exploded
    WHERE tier_value BETWEEN 36 AND 64
),
base AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(tier_value_cume_dist, 0) AS tier_value_cume_dist_filled
    FROM metrics
    WHERE MONTH(effective_from) IN (4, 5, 8)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        effective_from,
        tier_value_cume_dist,
        CUME_DIST() OVER (ORDER BY tier_value_cume_dist_filled NULLS FIRST) AS tier_value_cume_dist_filled_cume_dist,
        STDDEV_SAMP(tier_value_cume_dist) OVER (PARTITION BY DATE_TRUNC('YEAR', effective_from)) AS effective_from_period_stddev_samp_tier_value_cume_dist,
        CUME_DIST() OVER (ORDER BY tier_value_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_cume_dist,
        RANK() OVER (PARTITION BY YEAR(effective_from) ORDER BY tier_value_cume_dist_filled) AS tier_value_cume_dist_filled_rank,
        RANK() OVER (ORDER BY tier_value_cume_dist) AS tier_value_cume_dist_rank
    FROM base
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    ROUND(AVG(tier_value_cume_dist), 2) AS avg_tier_value_cume_dist,
    MIN(tier_value_cume_dist) AS min_tier_value_cume_dist,
    MIN(tier_value_cume_dist_rank) AS min_tier_value_cume_dist_rank,
    PERCENTILE_APPROX(tier_value_cume_dist, 0.95) AS tier_value_cume_dist_p95,
    GROUPING_ID() AS grouping_level
FROM scored
GROUP BY ROLLUP (DATE_TRUNC('MONTH', effective_from));
