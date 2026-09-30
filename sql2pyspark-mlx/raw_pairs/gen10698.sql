WITH exploded AS (
    SELECT id, EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
metrics AS (
    SELECT
        *,
        RANK() OVER (ORDER BY tier_value NULLS LAST) tier_value_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) tier_value_percent_rank
    FROM exploded
),
with_windows AS (
    SELECT
        id,
        tier_value_percent_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_percent_rank NULLS FIRST) tier_value_percent_rank_percent_rank,
        RANK() OVER (ORDER BY tier_value_rank DESC NULLS LAST) tier_value_rank_rank,
        CUME_DIST() OVER (ORDER BY tier_value_rank DESC NULLS LAST) tier_value_rank_cume_dist,
        RANK() OVER (ORDER BY tier_value_rank) tier_value_rank_rank_2,
        CUME_DIST() OVER (ORDER BY tier_value_rank DESC NULLS LAST) tier_value_rank_cume_dist_2
    FROM metrics
),
filtered AS (
    SELECT
        id,
        tier_value_percent_rank,
        tier_value_percent_rank_percent_rank,
        tier_value_rank_rank_2,
        tier_value_rank_cume_dist_2,
        ROUND(tier_value_rank_cume_dist * 1.15, 2) tier_value_rank_cume_dist_adjusted
    FROM with_windows
    WHERE tier_value_rank_cume_dist_2 > 0.38
),
valid AS (
    SELECT *, tier_value_percent_rank * tier_value_percent_rank_percent_rank tier_value_percent_rank_times_tier_value_percent_rank_percent_rank
    FROM filtered
    WHERE tier_value_rank_cume_dist_adjusted BETWEEN 24 AND 42
),
filtered_price_tiers AS (
    SELECT
        *,
        FLOOR(tier_value_percent_rank_times_tier_value_percent_rank_percent_rank) tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_whole,
        tier_value_rank_rank_2 + tier_value_percent_rank tier_value_rank_rank_2_plus_tier_value_percent_rank
    FROM valid
    WHERE tier_value_rank_cume_dist_adjusted >= 25
),
summary AS (
    SELECT
        CASE WHEN tier_value_rank_cume_dist_adjusted >= 68 THEN 'HIGH' ELSE 'LOW' END AS tier_value_rank_cume_dist_adjusted_tier,
        CASE WHEN tier_value_percent_rank_times_tier_value_percent_rank_percent_rank >= 70 THEN 'HIGH' ELSE 'LOW' END AS tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_tier,
        PERCENTILE_APPROX(tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_whole, ARRAY(0.25, 0.5, 0.75)) AS tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_whole_quartiles,
        COUNT(*) AS row_count,
        AVG(tier_value_percent_rank) AS avg_tier_value_percent_rank
    FROM filtered_price_tiers
    GROUP BY 1, 2
)
SELECT
    tier_value_rank_cume_dist_adjusted_tier,
    tier_value_percent_rank_times_tier_value_percent_rank_percent_rank_tier,
    row_count,
    avg_tier_value_percent_rank
FROM summary
ORDER BY tier_value_rank_cume_dist_adjusted_tier DESC;
