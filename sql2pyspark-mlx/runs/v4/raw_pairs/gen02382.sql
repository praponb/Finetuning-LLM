WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_recent AS tier_value_rank,
        CUME_DIST() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist,
        CUME_DIST() OVER (ORDER BY tier_value) AS tier_value_cume_dist_2
    FROM flattened
    WINDOW w_recent AS (ORDER BY tier_value DESC NULLS FIRST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        tier_value,
        tier_value_rank,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        CUME_DIST() OVER (ORDER BY tier_value_cume_dist DESC) AS tier_value_cume_dist_cume_dist,
        CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist_3,
        DENSE_RANK() OVER (ORDER BY tier_value_rank DESC NULLS LAST) AS tier_value_rank_dense_rank
    FROM scored
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY tier_value_cume_dist DESC) AS tier_value_cume_dist_cume_dist_2,
        DENSE_RANK() OVER (ORDER BY tier_value_cume_dist_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_cume_dist_dense_rank
    FROM with_windows
),
with_windows_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_ordered AS tier_value_rank_percent_rank,
        RANK() OVER (ORDER BY tier_value_rank_dense_rank NULLS LAST) AS tier_value_rank_dense_rank_rank,
        CUME_DIST() OVER (ORDER BY tier_value_cume_dist_2 DESC NULLS FIRST) AS tier_value_cume_dist_2_cume_dist
    FROM enriched
    WINDOW w_ordered AS (ORDER BY tier_value_rank DESC NULLS LAST)
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        tier_value,
        tier_value_rank,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        tier_value_cume_dist_3,
        tier_value_rank_dense_rank,
        tier_value_cume_dist_cume_dist_2,
        tier_value_rank_percent_rank,
        tier_value_rank_dense_rank_rank,
        tier_value_cume_dist_2 - tier_value_cume_dist_cume_dist_dense_rank AS tier_value_cume_dist_2_minus_tier_value_cume_dist_cume_dist_dense_rank
    FROM with_windows_price_tiers
    WHERE tier_value_cume_dist_cume_dist_dense_rank <= 4
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        tier_value,
        tier_value_rank,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        tier_value_cume_dist_3,
        tier_value_rank_dense_rank,
        tier_value_cume_dist_cume_dist_2,
        COALESCE(tier_value_cume_dist, 0) AS tier_value_cume_dist_filled,
        tier_value_cume_dist_2_minus_tier_value_cume_dist_cume_dist_dense_rank / NULLIF(tier_value_cume_dist_cume_dist_2, 0) AS tier_value_cume_dist_2_minus_tier_value_cume_dist_cume_dist_dense_rank_per_tier_value_cume_dist_cume_dist_2
    FROM filtered
    WHERE tier_value_cume_dist_2 BETWEEN 0.23 AND 0.49
)
-- Grouped metrics
SELECT
    CASE WHEN tier_value_cume_dist_filled >= 81 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_cume_dist_filled_tier,
    CASE WHEN tier_value_rank_dense_rank >= 6 THEN 'HIGH' ELSE 'LOW' END AS tier_value_rank_dense_rank_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(tier_value_cume_dist_filled), 2) AS avg_tier_value_cume_dist_filled,
    ROUND(AVG(tier_value_cume_dist_2), 2) AS avg_tier_value_cume_dist_2
FROM valid
GROUP BY tier_value_cume_dist_filled_tier, tier_value_rank_dense_rank_tier
ORDER BY tier_value_rank_dense_rank_tier
