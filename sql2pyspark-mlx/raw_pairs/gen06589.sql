WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        CEIL(discount_pct) AS discount_pct_whole,
        DAYOFWEEK(ends_on) AS ends_on_dayofweek
    FROM promotions
    WHERE starts_on >= DATE '2023-05-09'
      AND channel IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(promo_code) AS promo_code_length
    FROM valid
    WHERE starts_on > TO_DATE('2023-08-20')
      AND channel IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        starts_on,
        ends_on,
        ends_on_dayofweek,
        MIN(discount_pct_whole) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_discount_pct_whole,
        COUNT(discount_pct_whole) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_discount_pct_whole,
        SUM(discount_pct_whole) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_discount_pct_whole
    FROM cleaned
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        starts_on,
        ends_on_dayofweek,
        FIRST_VALUE(ends_on_dayofweek, TRUE) OVER (PARTITION BY promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ends_on_dayofweek,
        AVG(running_min_discount_pct_whole) OVER (PARTITION BY promo_code) AS promo_code_avg_running_min_discount_pct_whole
    FROM with_windows
),
cleaned_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        first_known_ends_on_dayofweek / NULLIF(promo_code_avg_running_min_discount_pct_whole, 0) AS first_known_ends_on_dayofweek_per_promo_code_avg_running_min_discount_pct_whole,
        first_known_ends_on_dayofweek - ends_on_dayofweek AS first_known_ends_on_dayofweek_minus_ends_on_dayofweek
    FROM enriched
    WHERE promo_code_avg_running_min_discount_pct_whole <= 69
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    metric,
    reading
FROM (
    SELECT
        id,
        first_known_ends_on_dayofweek_minus_ends_on_dayofweek,
        first_known_ends_on_dayofweek_per_promo_code_avg_running_min_discount_pct_whole
    FROM cleaned_promotions
) src
UNPIVOT (
    reading FOR metric IN (first_known_ends_on_dayofweek_minus_ends_on_dayofweek, first_known_ends_on_dayofweek_per_promo_code_avg_running_min_discount_pct_whole)
) unp;
