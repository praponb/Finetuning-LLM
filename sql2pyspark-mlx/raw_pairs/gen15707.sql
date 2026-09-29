WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        effective_from,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value
    FROM flattened
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(currency_max_tier_value) OVER (PARTITION BY YEAR(effective_from)) AS effective_from_year_stddev_samp_currency_max_tier_value,
        CUME_DIST() OVER (PARTITION BY YEAR(effective_from) ORDER BY tier_value_cume_dist DESC NULLS LAST) AS tier_value_cume_dist_cume_dist,
        COUNT(tier_value_cume_dist) OVER (PARTITION BY plan) AS plan_count_tier_value_cume_dist,
        CUME_DIST() OVER w_ordered AS tier_value_cume_dist_cume_dist_2
    FROM with_windows
    WINDOW w_ordered AS (ORDER BY tier_value_cume_dist NULLS FIRST)
),
scored AS (
    -- Ranking and gap features
    SELECT
        tier_value_cume_dist,
        plan_count_tier_value_cume_dist,
        tier_value_cume_dist_cume_dist_2,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY effective_from_year_stddev_samp_currency_max_tier_value NULLS LAST) AS effective_from_year_stddev_samp_currency_max_tier_value_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY currency_max_tier_value DESC) AS currency_max_tier_value_rank
    FROM enriched
    WHERE effective_from_year_stddev_samp_currency_max_tier_value BETWEEN 32 AND 68
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        plan_count_tier_value_cume_dist,
        tier_value_cume_dist_cume_dist_2,
        effective_from_year_stddev_samp_currency_max_tier_value_cume_dist,
        currency_max_tier_value_rank,
        ROUND(currency_max_tier_value_rank * 0.9, 2) AS currency_max_tier_value_rank_adjusted
    FROM scored
    WHERE tier_value_cume_dist_cume_dist_2 BETWEEN 0.23 AND 0.26
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY currency_max_tier_value_rank_adjusted NULLS FIRST) AS currency_max_tier_value_rank_adjusted_cume_dist,
        DENSE_RANK() OVER w_part AS currency_max_tier_value_rank_adjusted_dense_rank,
        CUME_DIST() OVER (ORDER BY currency_max_tier_value_rank DESC NULLS FIRST) AS currency_max_tier_value_rank_cume_dist,
        PERCENT_RANK() OVER (ORDER BY effective_from_year_stddev_samp_currency_max_tier_value_cume_dist NULLS FIRST) AS effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_percent_rank
    FROM cleaned
    WINDOW w_part AS (ORDER BY currency_max_tier_value_rank_adjusted DESC NULLS FIRST)
),
cleaned_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        plan_count_tier_value_cume_dist,
        tier_value_cume_dist_cume_dist_2,
        effective_from_year_stddev_samp_currency_max_tier_value_cume_dist,
        currency_max_tier_value_rank_adjusted,
        currency_max_tier_value_rank_adjusted_cume_dist,
        currency_max_tier_value_rank_adjusted_dense_rank,
        effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_percent_rank,
        LEAST(currency_max_tier_value_rank_adjusted_dense_rank, currency_max_tier_value_rank) AS least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank
    FROM metrics
    WHERE effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_percent_rank >= 0.3
)
-- Grouped metrics
SELECT
    CASE WHEN effective_from_year_stddev_samp_currency_max_tier_value_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END AS effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_tier,
    CASE WHEN plan_count_tier_value_cume_dist >= 18 THEN 'LARGE' ELSE 'SMALL' END AS plan_count_tier_value_cume_dist_tier,
    APPROX_PERCENTILE(effective_from_year_stddev_samp_currency_max_tier_value_cume_dist, 0.5) AS effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_p50,
    ROUND(AVG(least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank), 2) AS avg_least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank,
    PERCENTILE_APPROX(tier_value_cume_dist_cume_dist_2, 0.5) AS tier_value_cume_dist_cume_dist_2_p50,
    PERCENTILE_APPROX(least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank, ARRAY(0.25, 0.5, 0.75)) AS least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank_quartiles,
    PERCENTILE_APPROX(currency_max_tier_value_rank_adjusted, ARRAY(0.25, 0.5, 0.75)) AS currency_max_tier_value_rank_adjusted_quartiles,
    APPROX_PERCENTILE(effective_from_year_stddev_samp_currency_max_tier_value_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS effective_from_year_stddev_samp_currency_max_tier_value_cume_dist_quartiles
FROM cleaned_price_tiers
GROUP BY GROUPING SETS ((CASE WHEN effective_from_year_stddev_samp_currency_max_tier_value_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN plan_count_tier_value_cume_dist >= 18 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN effective_from_year_stddev_samp_currency_max_tier_value_cume_dist >= 0.81 THEN 'HIGH' ELSE 'LOW' END))
ORDER BY avg_least_currency_max_tier_value_rank_adjusted_dense_rank_currency_max_tier_value_rank DESC
