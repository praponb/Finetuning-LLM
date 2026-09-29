WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(product_name) AS product_name_length,
        COALESCE(brand, 'NONE') AS brand_filled
    FROM products
    WHERE supplier_id IS NOT NULL
      AND cost >= 320
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.store_sale_sale_date,
        top1.store_sale_revenue
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_filled,
        store_sale_sale_date,
        store_sale_revenue,
        ROW_NUMBER() OVER w_hist AS row_seq,
        AVG(cost) OVER (PARTITION BY supplier_id) AS supplier_id_avg_cost
    FROM with_latest
    WHERE brand = 'Acme'
    WINDOW w_hist AS (PARTITION BY DATE_TRUNC('MONTH', store_sale_sale_date), category ORDER BY store_sale_sale_date DESC, id)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        product_name,
        category,
        stock_qty,
        launch_date,
        tags,
        brand_filled,
        store_sale_sale_date,
        store_sale_revenue,
        row_seq,
        supplier_id_avg_cost,
        WEEKOFYEAR(launch_date) AS launch_date_weekofyear
    FROM scored
    WHERE ARRAY_CONTAINS(tags, 'sale')
)
-- Grouped metrics
SELECT
    CASE WHEN store_sale_revenue >= 4400 THEN 'LARGE' ELSE 'SMALL' END AS store_sale_revenue_tier,
    DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
    COUNT(DISTINCT category) AS distinct_categories,
    APPROX_PERCENTILE(launch_date_weekofyear, 0.5) AS launch_date_weekofyear_p50,
    SUM(CASE WHEN category = 'Electronics' THEN supplier_id_avg_cost ELSE 0 END) AS electronics_supplier_id_avg_cost,
    COUNT(CASE WHEN category = 'Electronics' THEN 1 END) AS electronics_count,
    RANK() OVER (PARTITION BY CASE WHEN store_sale_revenue >= 4400 THEN 'LARGE' ELSE 'SMALL' END ORDER BY SUM(CASE WHEN category = 'Electronics' THEN supplier_id_avg_cost ELSE 0 END) DESC) AS electronics_supplier_id_avg_cost_rank
FROM filtered
GROUP BY CASE WHEN store_sale_revenue >= 4400 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', launch_date);
