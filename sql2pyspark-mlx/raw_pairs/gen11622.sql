WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
base AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(plan) AS plan_length
    FROM exploded
    WHERE plan = 'enterprise'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan_length,
        COUNT(element) OVER (PARTITION BY plan, currency) AS plan_count_element,
        COUNT(plan_length) OVER w_hist AS plan_count_plan_length,
        COUNT(element) OVER w_hist AS plan_count_element_2
    FROM base
    WINDOW w_hist AS (PARTITION BY plan)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        plan_count_element,
        plan_count_element_2,
        DENSE_RANK() OVER (ORDER BY plan_length DESC NULLS FIRST) AS plan_length_dense_rank,
        DENSE_RANK() OVER (ORDER BY plan_count_element_2 DESC NULLS FIRST) AS plan_count_element_2_dense_rank,
        PERCENT_RANK() OVER (ORDER BY plan_length NULLS LAST) AS plan_length_percent_rank,
        CUME_DIST() OVER (ORDER BY plan_count_element DESC) AS plan_count_element_cume_dist
    FROM scored
),
base_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, FLOOR(plan_count_element_cume_dist) AS plan_count_element_cume_dist_whole
    FROM with_windows
    WHERE plan_count_element_cume_dist > 0.51
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan_count_element,
        plan_count_element_2,
        plan_count_element_2_dense_rank,
        plan_length_percent_rank,
        plan_count_element_cume_dist_whole,
        ROUND(plan_count_element_cume_dist, 1) AS plan_count_element_cume_dist_rounded
    FROM base_price_tiers
    WHERE plan_count_element_2_dense_rank = 3
),
base_price_tiers_2 AS (
    -- Basic cleansing and defaults
    SELECT *, plan_count_element - plan_count_element_2 AS plan_count_element_minus_plan_count_element_2
    FROM cleaned
    WHERE plan_count_element IS NOT NULL
)
-- Grouped metrics
SELECT
    CASE WHEN plan_count_element >= 15 THEN 'LARGE' ELSE 'SMALL' END AS plan_count_element_tier,
    CASE WHEN plan_count_element_2_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS plan_count_element_2_dense_rank_tier,
    ROUND(AVG(plan_count_element_2), 2) AS avg_plan_count_element_2,
    COUNT(*) AS row_count,
    COUNT(*) AS row_count_2,
    AVG(plan_length_percent_rank) AS avg_plan_length_percent_rank,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(plan_count_element_2, 0.9) AS plan_count_element_2_p90,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN plan_count_element >= 15 THEN 'LARGE' ELSE 'SMALL' END ORDER BY COUNT(*) DESC) AS row_count_2_percent_rank
FROM base_price_tiers_2
GROUP BY CASE WHEN plan_count_element >= 15 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN plan_count_element_2_dense_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY row_count DESC;
