WITH filtered AS (
    SELECT
        *,
        units - revenue AS revenue_per_unit,
        CASE WHEN units >= 34 THEN 'High' WHEN units >= 12 THEN 'Medium' ELSE 'Low' END AS units_band
    FROM store_sales
    WHERE store_id IN (3, 6)
),
with_dims AS (
    SELECT
        src.*,
        d.launch_date,
        d.price,
        d.stock_qty
    FROM filtered src
    LEFT JOIN products d
        ON src.product_id = d.id
),
enriched AS (
    SELECT
        e.id,
        e.product_id,
        e.sale_date,
        e.units,
        e.revenue,
        e.launch_date,
        e.price,
        e.stock_qty,
        d.stock_qty AS product_stock_qty,
        d.category
    FROM with_dims e
    LEFT JOIN products d
        ON e.product_id = d.id
),
scored AS (
    SELECT
        id,
        product_id,
        revenue,
        launch_date,
        price,
        product_stock_qty,
        ROW_NUMBER() OVER w_grp AS seq_no,
        ROUND(price / SUM(price) OVER (PARTITION BY product_id), 4) AS price_share
    FROM enriched
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY launch_date, id)
),
enriched_store_sales AS (
    SELECT
        e.*,
        d.cost,
        d.product_name,
        d.launch_date AS product_launch_date
    FROM scored e
    LEFT JOIN products d
        ON e.product_id = d.id
    WHERE d.brand IN ('Acme', 'Umbrella', 'Initech')
),
with_dims_store_sales AS (
    SELECT
        s.*,
        p.launch_date AS product_launch_date_2,
        p.stock_qty
    FROM enriched_store_sales s
    LEFT JOIN products p
        ON s.product_id = p.id
),
metrics AS (
    SELECT
        *,
        SUM(price) OVER (PARTITION BY product_id ORDER BY product_launch_date_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_price,
        AVG(price) OVER (PARTITION BY product_id ORDER BY product_launch_date_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_price,
        MIN(price) OVER (PARTITION BY product_id ORDER BY product_launch_date_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_price
    FROM with_dims_store_sales
),
summary AS (
    SELECT
        product_id,
        ROUND(AVG(product_stock_qty), 2) AS avg_product_stock_qty,
        MIN(cost) AS min_cost,
        APPROX_PERCENTILE(cost, 0.95) AS cost_p95
    FROM metrics
    GROUP BY ROLLUP (product_id)
)
SELECT product_id, min_cost
FROM summary
ORDER BY product_id;
