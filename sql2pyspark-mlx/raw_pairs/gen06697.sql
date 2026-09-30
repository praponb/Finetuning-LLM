WITH valid AS (
    SELECT *, DATE_TRUNC('year', sale_date) AS sale_date_period
    FROM store_sales
    WHERE store_id IN (1, 2, 5)
),
metrics AS (
    SELECT
        id,
        product_id,
        units,
        LEAD(revenue, 2, 0) OVER (PARTITION BY store_id ORDER BY sale_date_period, id) AS next_revenue,
        AVG(revenue) OVER (PARTITION BY TO_DATE(sale_date_period), store_id ORDER BY revenue, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS revenue_avg_5_prior
    FROM valid
    WHERE revenue < 1000
),
enriched AS (
    SELECT
        id,
        product_id,
        units,
        next_revenue,
        NTILE(10) OVER w_grp AS next_revenue_bucket,
        LAST_VALUE(next_revenue) IGNORE NULLS OVER w_grp_2 AS last_known_next_revenue,
        MAX(units) OVER w_running AS product_id_max_units
    FROM metrics
    WINDOW w_grp AS (ORDER BY next_revenue, id), w_grp_2 AS (PARTITION BY product_id ORDER BY next_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY product_id)
),
with_windows AS (
    SELECT
        *,
        LEAD(units, 2) OVER (PARTITION BY product_id ORDER BY next_revenue_bucket, id) AS next_units,
        STDDEV_SAMP(units) OVER (PARTITION BY product_id) AS product_id_stddev_samp_units,
        AVG(units) OVER (PARTITION BY product_id ORDER BY next_revenue_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS units_avg_6,
        MIN(last_known_next_revenue) OVER (PARTITION BY product_id) AS product_id_min_last_known_next_revenue
    FROM enriched
),
metrics_store_sales AS (
    SELECT
        id,
        product_id,
        next_revenue,
        next_revenue_bucket,
        product_id_max_units,
        product_id_min_last_known_next_revenue,
        NTILE(4) OVER w_hist AS next_revenue_bucket_2,
        AVG(product_id_max_units) OVER (ORDER BY product_id_min_last_known_next_revenue, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS product_id_max_units_avg_3_prior,
        CUME_DIST() OVER w_hist_2 AS product_id_max_units_cume_dist
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY next_revenue, id), w_hist_2 AS (ORDER BY product_id_max_units DESC)
)
SELECT
    id,
    product_id,
    next_revenue,
    product_id_min_last_known_next_revenue,
    MAX(product_id_max_units_avg_3_prior) OVER (PARTITION BY product_id ORDER BY product_id_max_units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_product_id_max_units_avg_3_prior,
    MIN(product_id_max_units_avg_3_prior) OVER (PARTITION BY product_id ORDER BY product_id_max_units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_product_id_max_units_avg_3_prior,
    SUM(product_id_max_units_avg_3_prior) OVER (PARTITION BY product_id ORDER BY product_id_max_units, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_product_id_max_units_avg_3_prior,
    LAST_VALUE(product_id_min_last_known_next_revenue) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY next_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_product_id_min_last_known_next_revenue
FROM metrics_store_sales
