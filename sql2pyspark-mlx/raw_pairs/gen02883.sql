WITH metrics AS (
    SELECT
        id,
        product_id,
        sale_date,
        ROUND(units / SUM(units) OVER (PARTITION BY store_id), 4) AS units_share,
        LAG(revenue, 2) OVER w_part AS prev_revenue,
        RANK() OVER (PARTITION BY store_id ORDER BY units NULLS FIRST) AS units_rank,
        AVG(revenue) OVER (ORDER BY sale_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS revenue_avg_2
    FROM store_sales
    WINDOW w_part AS (PARTITION BY DATE_TRUNC('week', sale_date), product_id ORDER BY sale_date, id)
),
enriched AS (
    SELECT
        e.id,
        e.product_id,
        e.prev_revenue,
        p.launch_date,
        p.price
    FROM metrics e
    JOIN products p
        ON e.product_id = p.id
    WHERE p.product_name LIKE 'Lap%'
),
with_windows AS (
    SELECT
        id,
        product_id,
        prev_revenue,
        launch_date,
        price,
        SUM(prev_revenue) OVER (PARTITION BY product_id ORDER BY prev_revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_prev_revenue,
        SUM(prev_revenue) OVER (PARTITION BY product_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_revenue,
        LEAD(price, 2) OVER (PARTITION BY MONTH(launch_date), product_id ORDER BY prev_revenue, id) AS next_price,
        LAST_VALUE(price) IGNORE NULLS OVER (PARTITION BY product_id ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_price,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY price, id) AS price_bucket
    FROM enriched
),
joined AS (
    SELECT
        e.id,
        e.product_id,
        e.price,
        e.cumulative_prev_revenue,
        e.last_known_price,
        e.price_bucket,
        p.category,
        p.stock_qty,
        p.launch_date AS product_launch_date
    FROM with_windows e
    JOIN products p
        ON e.product_id = p.id
),
metrics_store_sales AS (
    SELECT
        *,
        DENSE_RANK() OVER w_hist AS price_dense_rank,
        SUM(last_known_price) OVER w_running AS running_last_known_price,
        CUME_DIST() OVER w_running_2 AS stock_qty_cume_dist
    FROM joined
    WINDOW w_hist AS (ORDER BY price NULLS FIRST), w_running AS (PARTITION BY category ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY category ORDER BY stock_qty NULLS FIRST)
)
SELECT
    *,
    SUM(stock_qty_cume_dist) OVER (ORDER BY last_known_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty_cume_dist,
    AVG(stock_qty) OVER w_running AS stock_qty_avg_5_prior,
    ROUND(running_last_known_price / SUM(running_last_known_price) OVER (PARTITION BY category), 4) AS running_last_known_price_share
FROM metrics_store_sales
WINDOW w_running AS (PARTITION BY product_id ORDER BY running_last_known_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING);
