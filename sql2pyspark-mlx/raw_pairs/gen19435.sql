WITH base AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        launch_date,
        tags,
        supplier_id,
        FLOOR(stock_qty) AS stock_qty_whole,
        CAST(stock_qty AS DOUBLE) AS stock_qty_num
    FROM products
    WHERE launch_date >= DATE '2023-06-04'
)
SELECT
    id,
    product_name,
    brand,
    launch_date,
    supplier_id,
    stock_qty_whole,
    stock_qty_num,
    LAG(stock_qty_whole, 1) OVER (PARTITION BY category ORDER BY stock_qty_whole, id) AS prev_stock_qty_whole,
    CUME_DIST() OVER (PARTITION BY category, supplier_id ORDER BY stock_qty_num NULLS LAST) AS stock_qty_num_cume_dist,
    AVG(cost) OVER (PARTITION BY category ORDER BY launch_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6
FROM base;
