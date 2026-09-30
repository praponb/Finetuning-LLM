WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(revenue AS INT) AS revenue_int,
        ROUND(revenue * 100, 2) AS revenue_adjusted
    FROM store_sales
    WHERE sale_date > DATE '2023-04-16'
),
base_store_sales AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(revenue_int * 1.1, 2) AS adjusted_revenue_int,
        CASE WHEN revenue_int >= 69 THEN 'High' WHEN revenue_int >= 25 THEN 'Medium' ELSE 'Low' END AS revenue_int_band
    FROM base
    WHERE sale_date >= DATE '2023-07-27'
      AND YEAR(sale_date) >= 2023
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        product_id,
        units,
        revenue,
        revenue_int,
        revenue_adjusted,
        LAG(revenue) OVER w_recent AS prev_revenue,
        LEAD(revenue) OVER w_recent AS next_revenue,
        LAG(adjusted_revenue_int) OVER w_recent AS prev_adjusted_revenue_int,
        LEAD(adjusted_revenue_int) OVER w_recent AS next_adjusted_revenue_int,
        ROW_NUMBER() OVER w_recent AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date, id) AS event_seq_2
    FROM base_store_sales
    WHERE store_id IN (1, 3, 4, 5)
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY sale_date, id)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(4) OVER w_recent AS units_bucket,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY prev_adjusted_revenue_int DESC NULLS LAST) AS prev_adjusted_revenue_int_percent_rank
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY product_id ORDER BY units DESC, id)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.brand,
        p.launch_date
    FROM metrics a
    LEFT JOIN products p
        ON a.product_id = p.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        product_id,
        brand,
        next_adjusted_revenue_int
    FROM with_dims
) src
PIVOT (
    MAX(next_adjusted_revenue_int) AS total,
    COUNT(1) AS cnt
    FOR brand IN ('Acme' AS acme, 'Umbrella' AS umbrella, 'Initech' AS initech)
);
