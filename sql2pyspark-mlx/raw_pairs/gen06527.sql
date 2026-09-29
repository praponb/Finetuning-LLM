WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        sale_date,
        AVG(revenue) OVER w_running AS running_avg_revenue,
        MAX(revenue) OVER w_running AS running_max_revenue,
        SUM(revenue) OVER w_running AS running_sum_revenue,
        MIN(units) OVER (PARTITION BY product_id, store_id) AS product_id_min_units
    FROM store_sales
    WINDOW w_running AS (PARTITION BY store_id, product_id ORDER BY units, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        sale_date,
        running_avg_revenue,
        running_sum_revenue,
        product_id_min_units,
        LAG(running_avg_revenue, 1) OVER (PARTITION BY MONTH(sale_date) ORDER BY running_max_revenue, id) AS prev_running_avg_revenue,
        LAG(product_id_min_units, 1, 0) OVER (ORDER BY running_max_revenue, id) AS prev_product_id_min_units,
        AVG(product_id_min_units) OVER (ORDER BY sale_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS product_id_min_units_avg_5
    FROM scored
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        running_sum_revenue,
        product_id_min_units,
        prev_product_id_min_units,
        LAG(prev_product_id_min_units) OVER w_ordered AS prev_prev_product_id_min_units,
        LEAD(prev_product_id_min_units) OVER w_ordered AS next_prev_product_id_min_units,
        LEAD(product_id_min_units_avg_5) OVER w_ordered AS next_product_id_min_units_avg_5,
        ROW_NUMBER() OVER w_ordered AS seq_no
    FROM metrics
    WINDOW w_ordered AS (ORDER BY prev_running_avg_revenue, id)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        running_sum_revenue,
        product_id_min_units,
        prev_prev_product_id_min_units,
        next_product_id_min_units_avg_5,
        next_prev_product_id_min_units - running_sum_revenue AS next_prev_product_id_min_units_minus_running_sum_revenue
    FROM enriched
    WHERE product_id_min_units <= 40
)
SELECT
    id,
    running_sum_revenue,
    product_id_min_units,
    next_product_id_min_units_avg_5,
    next_prev_product_id_min_units_minus_running_sum_revenue
FROM base
ORDER BY product_id_min_units, id DESC
LIMIT 10
