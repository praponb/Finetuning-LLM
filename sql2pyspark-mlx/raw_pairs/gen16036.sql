WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(discounts) AS element
    FROM price_tiers
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(plan) plan_lower
    FROM lines
    WHERE plan NOT IN ('pro', 'basic', 'enterprise')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(plan) plan_lower_2
    FROM base
    WHERE plan IN ('pro', 'basic', 'enterprise')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        effective_from,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY element DESC NULLS FIRST) element_cume_dist
    FROM valid
    WHERE plan = 'basic'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT *, RANK() OVER w_hist element_cume_dist_rank
    FROM enriched
    WINDOW w_hist AS (ORDER BY element_cume_dist DESC NULLS FIRST)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY element_cume_dist DESC) element_cume_dist_cume_dist,
        RANK() OVER (ORDER BY element_cume_dist_rank DESC NULLS LAST) element_cume_dist_rank_rank,
        PERCENT_RANK() OVER (ORDER BY element_cume_dist) element_cume_dist_percent_rank,
        CUME_DIST() OVER (ORDER BY element_cume_dist NULLS FIRST) element_cume_dist_cume_dist_2
    FROM scored
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('YEAR', effective_from) AS year_start,
        CASE WHEN element_cume_dist_cume_dist_2 >= 0.87 THEN 'LARGE' ELSE 'SMALL' END AS element_cume_dist_cume_dist_2_tier,
        CASE WHEN element_cume_dist_cume_dist >= 0.7 THEN 'LARGE' ELSE 'SMALL' END AS element_cume_dist_cume_dist_tier,
        PERCENTILE_APPROX(element_cume_dist_rank_rank, ARRAY(0.25, 0.5, 0.75)) AS element_cume_dist_rank_rank_quartiles,
        PERCENTILE_APPROX(element_cume_dist_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS element_cume_dist_cume_dist_quartiles,
        PERCENTILE_APPROX(element_cume_dist_rank_rank, 0.5) AS element_cume_dist_rank_rank_p50
    FROM metrics
    GROUP BY CUBE (DATE_TRUNC('YEAR', effective_from), CASE WHEN element_cume_dist_cume_dist_2 >= 0.87 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN element_cume_dist_cume_dist >= 0.7 THEN 'LARGE' ELSE 'SMALL' END)
    HAVING PERCENTILE_APPROX(element_cume_dist_rank_rank, 0.5) > 0
)
SELECT
    element_cume_dist_cume_dist_2_tier,
    element_cume_dist_cume_dist_tier,
    element_cume_dist_cume_dist_quartiles,
    element_cume_dist_rank_rank_p50
FROM stats
