WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, units - revenue AS revenue_per_unit
    FROM store_sales
    WHERE sale_date >= TO_DATE('2024-03-01')
      AND store_id = 2
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(units) OVER (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
        DENSE_RANK() OVER (ORDER BY units NULLS FIRST) AS units_dense_rank,
        AVG(revenue) OVER (PARTITION BY store_id, product_id ORDER BY revenue_per_unit, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS revenue_avg_2_prior,
        PERCENT_RANK() OVER w_grp AS revenue_per_unit_percent_rank
    FROM base
    WINDOW w_grp AS (PARTITION BY store_id, product_id ORDER BY revenue_per_unit DESC)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN revenue_avg_2_prior >= 3749 THEN 'High' WHEN revenue_avg_2_prior >= 1544 THEN 'Medium' ELSE 'Low' END AS revenue_avg_2_prior_level,
        DATE_TRUNC('year', sale_date) AS sale_date_period
    FROM metrics
    WHERE revenue_per_unit < 33
),
with_dims AS (
    -- Attach products attributes
    SELECT src.*, p.stock_qty
    FROM valid src
    INNER JOIN products p
        ON src.product_id = p.id
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(sale_date_period) OVER (PARTITION BY store_id ORDER BY sale_date_period, id) AS prev_sale_date_period
    FROM with_dims
),
with_gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_sale_date_period IS NULL OR sale_date_period > prev_sale_date_period + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY store_id ORDER BY sale_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(prev_sale_date_period) OVER (PARTITION BY product_id ORDER BY prev_sale_date_period, id) IS NULL
                 OR prev_sale_date_period > LAG(prev_sale_date_period) OVER (PARTITION BY product_id ORDER BY prev_sale_date_period, id) + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM sessionized
),
sessionized_store_sales AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY product_id ORDER BY prev_sale_date_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
)
SELECT
    id,
    store_id,
    sale_date,
    revenue,
    revenue_per_unit_percent_rank,
    revenue_avg_2_prior_level,
    stock_qty,
    prev_sale_date_period,
    session_start_flag
FROM sessionized_store_sales
ORDER BY stock_qty DESC
