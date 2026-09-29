WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS element
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(element) OVER w_recent AS plan_count_element,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_dense_rank,
        SUM(element) OVER w_recent AS plan_sum_element,
        PERCENT_RANK() OVER w_part AS element_percent_rank,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('YEAR', effective_from) ORDER BY element DESC NULLS LAST) AS element_cume_dist
    FROM flattened
    WHERE MONTH(effective_from) IN (5, 8, 11)
    WINDOW w_recent AS (PARTITION BY plan), w_part AS (PARTITION BY plan ORDER BY element DESC)
),
metrics_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        plan_count_element,
        element_dense_rank,
        plan_sum_element,
        MIN(element_dense_rank) OVER (PARTITION BY plan) AS plan_min_element_dense_rank,
        CUME_DIST() OVER (ORDER BY plan_count_element NULLS LAST) AS plan_count_element_cume_dist,
        MIN(element_percent_rank) OVER (PARTITION BY plan) AS plan_min_element_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY MONTH(effective_from), plan ORDER BY element NULLS FIRST) AS element_percent_rank_2,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element_percent_rank DESC NULLS LAST) AS element_percent_rank_cume_dist
    FROM metrics
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(plan_min_element_percent_rank) AS plan_min_element_percent_rank_whole
    FROM metrics_price_tiers
    WHERE element_dense_rank = 5
),
enriched AS (
    -- Ranking and gap features
    SELECT
        plan_count_element_cume_dist,
        plan_min_element_percent_rank_whole,
        CUME_DIST() OVER (ORDER BY plan_min_element_dense_rank NULLS FIRST) AS plan_min_element_dense_rank_cume_dist,
        RANK() OVER (ORDER BY plan_count_element NULLS LAST) AS plan_count_element_rank,
        DENSE_RANK() OVER (ORDER BY plan_min_element_dense_rank DESC NULLS FIRST) AS plan_min_element_dense_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY plan_min_element_percent_rank) AS plan_min_element_percent_rank_percent_rank,
        PERCENT_RANK() OVER (ORDER BY plan_count_element_cume_dist DESC NULLS LAST) AS plan_count_element_cume_dist_percent_rank
    FROM filtered
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, plan_min_element_dense_rank_cume_dist / NULLIF(plan_count_element_cume_dist, 0) AS plan_min_element_dense_rank_cume_dist_per_plan_count_element_cume_dist
    FROM enriched
    WHERE plan_min_element_percent_rank_whole >= 62
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        plan_count_element_cume_dist,
        plan_min_element_percent_rank_whole,
        plan_count_element_rank,
        plan_min_element_percent_rank_percent_rank,
        plan_count_element_cume_dist_percent_rank,
        plan_count_element_cume_dist_percent_rank * plan_min_element_percent_rank_whole AS plan_count_element_cume_dist_percent_rank_times_plan_min_element_percent_rank_whole
    FROM cleaned
    WHERE plan_count_element_rank = 6
)
SELECT
    plan_count_element_cume_dist,
    plan_count_element_rank,
    plan_count_element_cume_dist_percent_rank,
    plan_count_element_cume_dist_percent_rank_times_plan_min_element_percent_rank_whole
FROM base
ORDER BY plan_count_element_cume_dist;
