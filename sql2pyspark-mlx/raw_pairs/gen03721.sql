WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        tier_value,
        COUNT(tier_value) OVER w_recent AS effective_from_year_count_tier_value
    FROM flattened
    WINDOW w_recent AS (PARTITION BY YEAR(effective_from), plan)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(tier_value) OVER w_grp AS effective_from_period_avg_tier_value,
        DENSE_RANK() OVER w_part AS tier_value_dense_rank,
        CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist
    FROM scored
    WINDOW w_grp AS (PARTITION BY DATE_TRUNC('week', effective_from)), w_part AS (ORDER BY tier_value NULLS FIRST)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank,
        RANK() OVER (ORDER BY tier_value_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_rank,
        CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist_2,
        RANK() OVER w_running AS effective_from_period_avg_tier_value_rank
    FROM metrics
    WINDOW w_running AS (PARTITION BY MONTH(effective_from) ORDER BY effective_from_period_avg_tier_value NULLS FIRST)
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER (ORDER BY effective_from_year_count_tier_value NULLS LAST) AS effective_from_year_count_tier_value_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC) AS tier_value_percent_rank_2,
        CUME_DIST() OVER (ORDER BY effective_from_period_avg_tier_value_rank DESC NULLS LAST) AS effective_from_period_avg_tier_value_rank_cume_dist
    FROM with_windows
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(tier_value_cume_dist_2 / tier_value_cume_dist_rank, 2) AS tier_value_cume_dist_2_per_tier_value_cume_dist_rank
    FROM scored_price_tiers
    WHERE effective_from > DATE '2024-04-29'
),
with_windows_price_tiers AS (
    -- Ranking and gap features
    SELECT
        effective_from,
        tier_value,
        effective_from_year_count_tier_value,
        tier_value_dense_rank,
        tier_value_cume_dist,
        tier_value_percent_rank,
        tier_value_cume_dist_rank,
        tier_value_cume_dist_2,
        effective_from_period_avg_tier_value_rank,
        effective_from_year_count_tier_value_rank,
        tier_value_percent_rank_2,
        tier_value_cume_dist_2_per_tier_value_cume_dist_rank,
        RANK() OVER (ORDER BY tier_value_percent_rank DESC) AS tier_value_percent_rank_rank,
        RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_rank,
        CUME_DIST() OVER (ORDER BY tier_value_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_cume_dist,
        RANK() OVER (ORDER BY effective_from_year_count_tier_value_rank DESC NULLS LAST) AS effective_from_year_count_tier_value_rank_rank,
        RANK() OVER (ORDER BY tier_value_dense_rank NULLS LAST) AS tier_value_dense_rank_rank
    FROM cleaned
)
SELECT
    effective_from,
    effective_from_year_count_tier_value,
    tier_value_dense_rank,
    tier_value_cume_dist,
    tier_value_cume_dist_rank,
    effective_from_year_count_tier_value_rank,
    tier_value_cume_dist_2_per_tier_value_cume_dist_rank,
    tier_value_percent_rank_rank,
    tier_value_rank
FROM with_windows_price_tiers;
