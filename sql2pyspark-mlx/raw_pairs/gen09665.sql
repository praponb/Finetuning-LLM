WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS element
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(element) OVER w_ordered AS currency_min_element,
        CUME_DIST() OVER w_hist AS element_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY element) AS element_cume_dist_2
    FROM exploded
    WINDOW w_ordered AS (PARTITION BY currency, plan), w_hist AS (PARTITION BY plan ORDER BY element)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        currency,
        element,
        currency_min_element,
        element_cume_dist,
        element_cume_dist_2,
        GREATEST(currency_min_element, element) AS greatest_currency_min_element_element,
        element_cume_dist + element_cume_dist_2 AS element_cume_dist_plus_element_cume_dist_2
    FROM with_windows
    WHERE element <= 28
),
with_windows_price_tiers AS (
    -- Running and moving aggregates
    SELECT *, CUME_DIST() OVER (PARTITION BY currency ORDER BY greatest_currency_min_element_element NULLS FIRST) AS greatest_currency_min_element_element_cume_dist
    FROM base
),
with_windows_price_tiers_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY greatest_currency_min_element_element DESC) AS greatest_currency_min_element_element_rank,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY element DESC) AS element_cume_dist_3,
        CUME_DIST() OVER (ORDER BY element_cume_dist_plus_element_cume_dist_2 DESC NULLS LAST) AS element_cume_dist_plus_element_cume_dist_2_cume_dist
    FROM with_windows_price_tiers
    WHERE currency_min_element < 71
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        currency,
        plan,
        element_cume_dist
    FROM with_windows_price_tiers_2
) src
PIVOT (
    SUM(element_cume_dist) AS value_sum,
    COUNT(1) AS item_count
    FOR plan IN ('pro' AS pro, 'basic' AS basic)
)
