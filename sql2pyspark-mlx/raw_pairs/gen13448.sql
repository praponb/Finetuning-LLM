WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_prices) AS element
    FROM price_tiers
),
filtered AS (
    SELECT *, CEIL(element) AS element_whole
    FROM exploded
    WHERE currency = 'THB'
),
cleaned AS (
    SELECT
        id,
        plan,
        currency,
        element,
        element_whole,
        element + element_whole AS element_plus_element_whole,
        element + element_whole AS element_plus_element_whole_2
    FROM filtered
    WHERE plan NOT IN ('pro', 'starter')
),
scored AS (
    SELECT
        element_plus_element_whole,
        element_plus_element_whole_2,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY element_plus_element_whole DESC) AS element_plus_element_whole_cume_dist,
        COUNT(element_whole) OVER (PARTITION BY plan) AS plan_count_element_whole,
        MIN(element_plus_element_whole_2) OVER (PARTITION BY currency) AS currency_min_element_plus_element_whole_2
    FROM cleaned
),
base AS (
    SELECT
        element_plus_element_whole,
        element_plus_element_whole_cume_dist,
        plan_count_element_whole,
        currency_min_element_plus_element_whole_2,
        element_plus_element_whole_2 + plan_count_element_whole AS element_plus_element_whole_2_plus_plan_count_element_whole
    FROM scored
    WHERE element_plus_element_whole_2 < 21
)
SELECT
    CASE WHEN element_plus_element_whole_cume_dist >= 0.87 THEN 'HIGH' ELSE 'LOW' END AS element_plus_element_whole_cume_dist_tier,
    PERCENTILE_APPROX(element_plus_element_whole, ARRAY(0.25, 0.5, 0.75)) AS element_plus_element_whole_quartiles,
    ROUND(AVG(element_plus_element_whole), 2) AS avg_element_plus_element_whole,
    COUNT(*) AS num_rows,
    COUNT(*) AS num_rows_2
FROM base
GROUP BY CASE WHEN element_plus_element_whole_cume_dist >= 0.87 THEN 'HIGH' ELSE 'LOW' END
ORDER BY avg_element_plus_element_whole NULLS LAST
