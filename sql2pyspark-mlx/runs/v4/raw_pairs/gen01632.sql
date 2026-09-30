WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS tier_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_running AS tier_value_dense_rank,
        CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist,
        DENSE_RANK() OVER w_running AS tier_value_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank_3
    FROM lines
    WINDOW w_running AS (ORDER BY tier_value)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, tier_value_dense_rank * tier_value AS tier_value_dense_rank_times_tier_value
    FROM metrics
    WHERE effective_from > DATE '2024-07-06'
)
-- Grouped metrics
SELECT
    CASE WHEN tier_value_dense_rank >= 8 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_dense_rank_tier,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    AVG(tier_value_cume_dist) AS avg_tier_value_cume_dist,
    COUNT(DISTINCT id) AS distinct_ids
FROM valid
GROUP BY 1, 2
ORDER BY quarter_start NULLS FIRST
