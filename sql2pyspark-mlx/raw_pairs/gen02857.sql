WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY element) AS element_percent_rank,
        DENSE_RANK() OVER w_ordered AS element_dense_rank,
        COUNT(element) OVER w_ordered_2 AS effective_from_month_count_element,
        AVG(element) OVER (PARTITION BY currency) AS currency_avg_element,
        DENSE_RANK() OVER (ORDER BY element DESC NULLS LAST) AS element_dense_rank_2
    FROM lines
    WHERE effective_from BETWEEN '2023-12-01' AND '2024-04-17'
    WINDOW w_ordered AS (ORDER BY element DESC), w_ordered_2 AS (PARTITION BY MONTH(effective_from), currency)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        currency,
        element,
        element_percent_rank,
        element_dense_rank,
        effective_from_month_count_element,
        currency_avg_element,
        element_dense_rank_2,
        DAY(effective_from) AS effective_from_day,
        CASE WHEN element_percent_rank >= 0.87 THEN 'High' WHEN element_percent_rank >= 0.13 THEN 'Medium' ELSE 'Low' END AS element_percent_rank_level
    FROM with_windows
    WHERE currency IN ('EUR', 'USD')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        effective_from,
        element_percent_rank,
        effective_from_month_count_element,
        currency_avg_element,
        effective_from_day,
        element_percent_rank_level,
        ROUND(effective_from_day, 2) AS effective_from_day_rounded,
        effective_from_month_count_element - element AS effective_from_month_count_element_minus_element
    FROM cleaned
    WHERE effective_from >= TO_DATE('2024-04-14')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        element_percent_rank,
        effective_from_month_count_element,
        currency_avg_element,
        effective_from_day,
        effective_from_day_rounded,
        effective_from_month_count_element_minus_element,
        CEIL(effective_from_day) AS effective_from_day_whole
    FROM base
    WHERE effective_from >= '2025-06-01'
),
cleaned_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN effective_from_day_rounded >= 84 THEN 'High' WHEN effective_from_day_rounded >= 16 THEN 'Medium' ELSE 'Low' END AS effective_from_day_rounded_band
    FROM valid
    WHERE effective_from_day_whole = 50
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN currency_avg_element >= 81 THEN 'HIGH' ELSE 'LOW' END AS currency_avg_element_tier,
        CASE WHEN effective_from_month_count_element_minus_element >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS effective_from_month_count_element_minus_element_tier,
        PERCENTILE_APPROX(effective_from_day, 0.95) AS effective_from_day_p95,
        APPROX_PERCENTILE(effective_from_month_count_element_minus_element, ARRAY(0.25, 0.5, 0.75)) AS effective_from_month_count_element_minus_element_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN currency_avg_element >= 81 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT(DISTINCT id) DESC) AS distinct_ids_percent_rank
    FROM cleaned_price_tiers
    GROUP BY 1, 2
)
SELECT
    currency_avg_element_tier,
    effective_from_month_count_element_minus_element_tier,
    effective_from_day_p95,
    distinct_ids_percent_rank
FROM daily
