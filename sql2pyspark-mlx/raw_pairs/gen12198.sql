WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS element
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element_2,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_percent_rank,
        STDDEV_SAMP(element) OVER (PARTITION BY plan) AS plan_stddev_samp_element_3,
        COUNT(element) OVER (PARTITION BY plan) AS plan_count_element
    FROM lines
    WHERE plan = 'enterprise'
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, element_percent_rank / NULLIF(plan_stddev_samp_element_2, 0) AS element_percent_rank_per_plan_stddev_samp_element_2
    FROM metrics
    WHERE element_percent_rank > 0.46
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, plan_stddev_samp_element_3 - element AS plan_stddev_samp_element_3_minus_element
    FROM base
    WHERE element_percent_rank_per_plan_stddev_samp_element_2 >= 58
),
filtered_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        *,
        element / NULLIF(plan_stddev_samp_element_3, 0) AS element_per_plan_stddev_samp_element_3,
        ROUND(element_percent_rank_per_plan_stddev_samp_element_2 * 0.85, 2) AS element_percent_rank_per_plan_stddev_samp_element_2_adjusted
    FROM filtered
    WHERE element_percent_rank_per_plan_stddev_samp_element_2 > 67
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, plan_stddev_samp_element_2 - plan_stddev_samp_element_3_minus_element AS plan_stddev_samp_element_2_minus_plan_stddev_samp_element_3_minus_element
    FROM filtered_price_tiers
    WHERE plan_stddev_samp_element_2 = 38
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LEAST(element, plan_stddev_samp_element_2) AS least_element_plan_stddev_samp_element_2,
        COALESCE(plan_count_element, 0) AS plan_count_element_filled
    FROM cleaned
    WHERE element_percent_rank_per_plan_stddev_samp_element_2_adjusted <= 69
)
-- Grouped metrics
SELECT
    plan,
    COUNT(CASE WHEN plan = 'pro' THEN 1 END) AS pro_count,
    ROUND(AVG(element_per_plan_stddev_samp_element_3), 2) AS avg_element_per_plan_stddev_samp_element_3,
    MAX(plan_count_element) FILTER (WHERE plan = 'starter') AS starter_max_plan_count_element,
    MIN(least_element_plan_stddev_samp_element_2) AS min_least_element_plan_stddev_samp_element_2,
    APPROX_PERCENTILE(element_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS element_percent_rank_quartiles,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM valid
GROUP BY plan
ORDER BY avg_element_per_plan_stddev_samp_element_3 NULLS LAST
