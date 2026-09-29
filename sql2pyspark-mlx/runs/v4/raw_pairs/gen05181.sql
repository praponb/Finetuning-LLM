WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS element
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, TRIM(plan) AS clean_plan
    FROM lines
    WHERE effective_from > TO_DATE('2024-07-09')
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(element) OVER (PARTITION BY plan) AS plan_sum_element,
        SUM(element) OVER (PARTITION BY plan) AS plan_sum_element_2,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY element DESC NULLS LAST) AS element_percent_rank
    FROM valid
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM scored
    INTERSECT
    SELECT id
    FROM price_tiers
    WHERE currency = 'USD'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM scored s
    INNER JOIN remaining_keys k ON s.id = k.id
),
stats AS (
    -- Grouped metrics
    SELECT
        plan,
        MIN(plan_sum_element) AS min_plan_sum_element,
        SUM(CASE WHEN plan = 'pro' THEN element_percent_rank ELSE 0 END) AS pro_element_percent_rank,
        COUNT(DISTINCT plan) AS distinct_plans,
        COUNT(DISTINCT plan) AS distinct_plans_2,
        COUNT(DISTINCT plan) AS distinct_plans_3
    FROM verified
    GROUP BY plan
)
SELECT
    plan,
    min_plan_sum_element,
    pro_element_percent_rank,
    distinct_plans_2,
    distinct_plans_3
FROM stats
ORDER BY distinct_plans_2;
