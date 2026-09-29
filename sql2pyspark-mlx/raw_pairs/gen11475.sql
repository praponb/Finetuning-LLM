WITH filtered AS (
    -- Keep recent, valid rows
    SELECT *, DATE_TRUNC('year', sale_date) AS sale_date_period
    FROM store_sales
    WHERE product_id IN (2, 4, 10)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) AS row_num
    FROM filtered
    QUALIFY row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(units) OVER (PARTITION BY product_id ORDER BY revenue, id) AS prev_units,
        LAG(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id) AS prev_revenue,
        LEAD(revenue) OVER (PARTITION BY product_id ORDER BY revenue, id) AS next_revenue,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY revenue, id) AS seq_no,
        FIRST_VALUE(units, TRUE) OVER (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_units
    FROM ranked
    WHERE units BETWEEN 26 AND 32
),
ranked_store_sales AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC NULLS LAST, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM ranked_store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY next_revenue DESC NULLS FIRST, id) = 1
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    store_id,
    kpi,
    metric_value
FROM (
    SELECT
        id,
        store_id,
        revenue,
        prev_revenue,
        next_revenue
    FROM latest
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR kpi IN (revenue, prev_revenue, next_revenue)
)
