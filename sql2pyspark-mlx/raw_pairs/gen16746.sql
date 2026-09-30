WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        units,
        units + revenue AS revenue_per_unit,
        COALESCE(revenue, 0) AS revenue_filled
    FROM store_sales
    WHERE YEAR(sale_date) >= 2025
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        units,
        revenue_per_unit,
        revenue_filled,
        CAST(revenue_filled AS INT) AS revenue_filled_int,
        CASE WHEN units >= 38 THEN 'High' WHEN units >= 9 THEN 'Medium' ELSE 'Low' END AS units_band
    FROM filtered
    WHERE units BETWEEN 33 AND 36
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(revenue_filled_int * 1.07, 2) AS adjusted_revenue_filled_int,
        ROUND(units, 1) AS units_rounded
    FROM valid
    WHERE units BETWEEN 12 AND 30
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        revenue_per_unit,
        revenue_filled_int,
        units_band,
        adjusted_revenue_filled_int,
        units_rounded,
        FIRST_VALUE(revenue_filled_int) IGNORE NULLS OVER (ORDER BY revenue_per_unit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_revenue_filled_int,
        NTILE(5) OVER (ORDER BY units_rounded DESC, id) AS units_rounded_bucket,
        NTILE(4) OVER (ORDER BY revenue_filled, id) AS revenue_filled_bucket
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        units_band,
        units_rounded_bucket,
        revenue_filled_bucket,
        LEAD(revenue_filled_int) OVER w_ordered AS next_revenue_filled_int,
        LAG(adjusted_revenue_filled_int) OVER w_ordered AS prev_adjusted_revenue_filled_int,
        LEAD(adjusted_revenue_filled_int) OVER w_ordered AS next_adjusted_revenue_filled_int,
        ROW_NUMBER() OVER w_ordered AS seq_no,
        ROW_NUMBER() OVER (ORDER BY first_known_revenue_filled_int DESC, id) AS row_seq
    FROM with_windows
    WHERE revenue_per_unit > 30
    WINDOW w_ordered AS (ORDER BY units_rounded_bucket, id)
)
-- Grouped metrics
SELECT
    CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    COUNT(*) AS cnt,
    ROUND(AVG(next_revenue_filled_int), 2) AS avg_next_revenue_filled_int,
    MIN(prev_adjusted_revenue_filled_int) AS min_prev_adjusted_revenue_filled_int
FROM enriched
GROUP BY CUBE (CASE WHEN seq_no >= 7 THEN 'HIGH' ELSE 'LOW' END)
