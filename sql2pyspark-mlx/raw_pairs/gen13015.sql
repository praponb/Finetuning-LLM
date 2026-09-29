WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS tier_value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        tier_value,
        CUME_DIST() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist,
        RANK() OVER w_recent AS tier_value_rank,
        CUME_DIST() OVER w_recent AS tier_value_cume_dist_2,
        DENSE_RANK() OVER w_recent AS tier_value_dense_rank
    FROM lines
    WINDOW w_recent AS (ORDER BY tier_value DESC)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        tier_value_cume_dist,
        tier_value_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC) AS tier_value_percent_rank,
        CUME_DIST() OVER (ORDER BY tier_value_dense_rank NULLS LAST) AS tier_value_dense_rank_cume_dist,
        RANK() OVER (ORDER BY tier_value_cume_dist) AS tier_value_cume_dist_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist DESC) AS tier_value_cume_dist_percent_rank
    FROM enriched
    WHERE tier_value_cume_dist_2 <= 0.7
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, tier_value_rank + tier_value_cume_dist_percent_rank AS tier_value_rank_plus_tier_value_cume_dist_percent_rank
    FROM metrics
    WHERE tier_value_percent_rank BETWEEN 0.39 AND 0.41
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        tier_value_cume_dist,
        tier_value_dense_rank_cume_dist,
        tier_value_cume_dist_percent_rank,
        tier_value_rank_plus_tier_value_cume_dist_percent_rank,
        DENSE_RANK() OVER (ORDER BY tier_value_dense_rank_cume_dist DESC) AS tier_value_dense_rank_cume_dist_dense_rank
    FROM valid
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        tier_value_dense_rank_cume_dist,
        tier_value_cume_dist_percent_rank,
        tier_value_rank_plus_tier_value_cume_dist_percent_rank,
        tier_value_dense_rank_cume_dist_dense_rank,
        tier_value_rank_plus_tier_value_cume_dist_percent_rank + tier_value_cume_dist_percent_rank AS tier_value_rank_plus_tier_value_cume_dist_percent_rank_plus_tier_value_cume_dist_percent_rank
    FROM metrics_price_tiers
    WHERE tier_value_cume_dist_percent_rank >= 0.2
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value_cume_dist_percent_rank >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_cume_dist_percent_rank_tier,
    CASE WHEN tier_value_dense_rank_cume_dist >= 0.61 THEN 'HIGH' ELSE 'LOW' END AS tier_value_dense_rank_cume_dist_tier,
    CASE WHEN tier_value_rank_plus_tier_value_cume_dist_percent_rank >= 89 THEN 'HIGH' ELSE 'LOW' END AS tier_value_rank_plus_tier_value_cume_dist_percent_rank_tier,
    ROUND(AVG(tier_value_rank_plus_tier_value_cume_dist_percent_rank), 2) AS avg_tier_value_rank_plus_tier_value_cume_dist_percent_rank,
    PERCENTILE_APPROX(tier_value_dense_rank_cume_dist, 0.9) AS tier_value_dense_rank_cume_dist_p90,
    APPROX_PERCENTILE(tier_value_rank_plus_tier_value_cume_dist_percent_rank_plus_tier_value_cume_dist_percent_rank, 0.5) AS tier_value_rank_plus_tier_value_cume_dist_percent_rank_plus_tier_value_cume_dist_percent_rank_p50,
    ROUND(AVG(tier_value_dense_rank_cume_dist_dense_rank), 2) AS avg_tier_value_dense_rank_cume_dist_dense_rank,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN tier_value_cume_dist_percent_rank >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY PERCENTILE_APPROX(tier_value_dense_rank_cume_dist, 0.9) DESC) AS tier_value_dense_rank_cume_dist_p90_percent_rank
FROM filtered
GROUP BY CASE WHEN tier_value_cume_dist_percent_rank >= 0.83 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN tier_value_dense_rank_cume_dist >= 0.61 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN tier_value_rank_plus_tier_value_cume_dist_percent_rank >= 89 THEN 'HIGH' ELSE 'LOW' END;
