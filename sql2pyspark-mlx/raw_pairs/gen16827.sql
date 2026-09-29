WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS element
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        DENSE_RANK() OVER (ORDER BY element DESC) AS element_dense_rank
    FROM flattened
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(element_dense_rank, 2) AS element_dense_rank_rounded
    FROM metrics
    WHERE plan IN ('enterprise', 'pro', 'starter')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(plan) AS plan_lower
    FROM cleaned
    WHERE element_dense_rank_rounded BETWEEN 29 AND 40
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(element_dense_rank) OVER w_ordered AS plan_sum_element_dense_rank,
        AVG(element_dense_rank_rounded) OVER w_ordered AS plan_avg_element_dense_rank_rounded,
        AVG(element_dense_rank_rounded) OVER w_ordered AS plan_avg_element_dense_rank_rounded_2,
        STDDEV_SAMP(element_dense_rank) OVER w_ordered AS plan_stddev_samp_element_dense_rank
    FROM valid
    WINDOW w_ordered AS (PARTITION BY plan)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        plan,
        ROUND(SUM(plan_avg_element_dense_rank_rounded_2) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS plan_avg_element_dense_rank_rounded_2_per_enterprise,
        MAX(element_dense_rank_rounded) FILTER (WHERE plan = 'starter') AS starter_max_element_dense_rank_rounded,
        AVG(element_dense_rank) FILTER (WHERE plan = 'basic') AS basic_avg_element_dense_rank,
        ROUND(SUM(plan_avg_element_dense_rank_rounded) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS plan_avg_element_dense_rank_rounded_per_basic
    FROM enriched
    GROUP BY plan
    HAVING plan_avg_element_dense_rank_rounded_per_basic >= 0
)
SELECT
    plan,
    starter_max_element_dense_rank_rounded,
    basic_avg_element_dense_rank,
    plan_avg_element_dense_rank_rounded_per_basic,
    CASE WHEN plan RLIKE '^(basic|enterprise)$' THEN 'CORE' ELSE 'PRIORITY' END AS plan_zone
FROM summary
ORDER BY plan_zone NULLS LAST;
