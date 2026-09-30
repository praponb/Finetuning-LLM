WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER w_running AS tier_value_dense_rank,
        PERCENT_RANK() OVER w_running AS tier_value_percent_rank,
        DENSE_RANK() OVER w_running AS tier_value_dense_rank_2
    FROM exploded
    WHERE tier_value > 62
    WINDOW w_running AS (ORDER BY tier_value DESC)
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(tier_value_percent_rank AS INT) AS tier_value_percent_rank_int,
        ROUND(tier_value, 2) AS tier_value_rounded
    FROM scored
    WHERE tier_value IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        tier_value,
        tier_value_dense_rank,
        tier_value_percent_rank,
        tier_value_dense_rank_2,
        tier_value_percent_rank_int,
        tier_value_rounded,
        tier_value_dense_rank_2 - tier_value_dense_rank AS tier_value_dense_rank_2_minus_tier_value_dense_rank
    FROM cleaned
    WHERE tier_value_percent_rank >= 0.22
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value_dense_rank_2_minus_tier_value_dense_rank >= 68 THEN 'HIGH' ELSE 'LOW' END AS tier_value_dense_rank_2_minus_tier_value_dense_rank_tier,
    CASE WHEN tier_value_percent_rank_int >= 82 THEN 'HIGH' ELSE 'LOW' END AS tier_value_percent_rank_int_tier,
    PERCENTILE_APPROX(tier_value_percent_rank_int, 0.5) AS tier_value_percent_rank_int_p50,
    APPROX_PERCENTILE(tier_value_percent_rank_int, ARRAY(0.25, 0.5, 0.75)) AS tier_value_percent_rank_int_quartiles,
    COUNT(*) AS cnt,
    GROUPING_ID() AS grouping_level
FROM filtered
GROUP BY GROUPING SETS ((CASE WHEN tier_value_dense_rank_2_minus_tier_value_dense_rank >= 68 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN tier_value_percent_rank_int >= 82 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN tier_value_dense_rank_2_minus_tier_value_dense_rank >= 68 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY tier_value_dense_rank_2_minus_tier_value_dense_rank_tier DESC;
