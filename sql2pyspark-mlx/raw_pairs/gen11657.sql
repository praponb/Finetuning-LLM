WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), effective_from) / 365 AS years_since_effective_from,
        CEIL(tier_value) AS tier_value_whole
    FROM exploded
    WHERE tier_value >= 33
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank,
        COUNT(tier_value_whole) OVER (PARTITION BY plan) AS plan_count_tier_value_whole
    FROM filtered
    WHERE effective_from >= '2023-10-11'
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        tier_value,
        years_since_effective_from,
        tier_value_whole,
        DENSE_RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY years_since_effective_from NULLS LAST) AS years_since_effective_from_dense_rank,
        CUME_DIST() OVER (PARTITION BY YEAR(effective_from), plan ORDER BY tier_value DESC) AS tier_value_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_count_tier_value_whole DESC NULLS FIRST) AS plan_count_tier_value_whole_cume_dist
    FROM scored
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY YEAR(effective_from) ORDER BY plan_count_tier_value_whole_cume_dist) AS plan_count_tier_value_whole_cume_dist_dense_rank,
        CUME_DIST() OVER (ORDER BY plan_count_tier_value_whole_cume_dist DESC NULLS FIRST) AS plan_count_tier_value_whole_cume_dist_cume_dist,
        PERCENT_RANK() OVER (ORDER BY years_since_effective_from_dense_rank DESC NULLS LAST) AS years_since_effective_from_dense_rank_percent_rank
    FROM scored_price_tiers
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        tier_value,
        tier_value_whole,
        tier_value_cume_dist,
        plan_count_tier_value_whole_cume_dist,
        plan_count_tier_value_whole_cume_dist_dense_rank,
        plan_count_tier_value_whole_cume_dist_cume_dist,
        years_since_effective_from_dense_rank_percent_rank,
        FLOOR(years_since_effective_from_dense_rank) AS years_since_effective_from_dense_rank_whole
    FROM with_windows
    WHERE tier_value_cume_dist BETWEEN 0.31 AND 0.45
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN plan_count_tier_value_whole_cume_dist_cume_dist >= 0.6 THEN 'HIGH' ELSE 'LOW' END AS plan_count_tier_value_whole_cume_dist_cume_dist_tier,
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    AVG(tier_value) AS avg_tier_value,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(plan_count_tier_value_whole_cume_dist_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS plan_count_tier_value_whole_cume_dist_dense_rank_quartiles,
    GROUPING_ID() AS grouping_level
FROM valid
GROUP BY CUBE (CASE WHEN plan_count_tier_value_whole_cume_dist_cume_dist >= 0.6 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('QUARTER', effective_from))
ORDER BY plan_count_tier_value_whole_cume_dist_cume_dist_tier DESC
