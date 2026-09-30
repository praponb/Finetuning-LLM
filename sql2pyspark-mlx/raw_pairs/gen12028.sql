WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        units,
        revenue,
        CEIL(revenue) revenue_whole,
        ROUND(revenue / units, 2) revenue_per_unit
    FROM store_sales
    WHERE MONTH(sale_date) IN (1, 7, 11)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(revenue_per_unit) OVER w_running last3_count_revenue_per_unit,
        SUM(revenue_per_unit) OVER w_running last3_sum_revenue_per_unit,
        MAX(revenue_per_unit) OVER w_running last3_max_revenue_per_unit,
        SUM(units) OVER (ORDER BY revenue ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_units
    FROM cleaned
    WHERE store_id = 4
    WINDOW w_running AS (PARTITION BY store_id ORDER BY revenue_per_unit ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Attach stores attributes
    SELECT s.*, p2.sqft
    FROM scored s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sqft ASC NULLS LAST, id ASC) AS dedup_rank
        FROM enriched
    ) ranked
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        units,
        revenue_whole,
        last3_sum_revenue_per_unit,
        cumulative_units,
        sqft,
        SUM(last3_sum_revenue_per_unit) OVER (PARTITION BY store_id ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_last3_sum_revenue_per_unit,
        LAG(sqft, 2, 0) OVER (ORDER BY revenue ASC, id ASC) prev_sqft,
        LAG(revenue, 2) OVER (ORDER BY revenue ASC, id ASC) prev_revenue
    FROM ranked
)
SELECT
    id,
    units,
    last3_sum_revenue_per_unit,
    sqft,
    prev_sqft,
    prev_revenue
FROM with_windows
ORDER BY prev_sqft DESC, sqft DESC
