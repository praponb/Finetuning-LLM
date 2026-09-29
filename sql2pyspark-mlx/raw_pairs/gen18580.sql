WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LEAST(stock_qty, price) AS least_stock_qty_price,
        LENGTH(brand) AS brand_length
    FROM products
    WHERE supplier_id IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        brand,
        cost,
        launch_date,
        tags,
        supplier_id,
        least_stock_qty_price,
        UPPER(category) AS category_upper,
        ARRAY_CONTAINS(tags, 'bestseller') AS is_bestseller
    FROM base
    WHERE launch_date > DATE '2023-06-19'
      AND brand IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        launch_date,
        tags,
        LAST_VALUE(least_stock_qty_price) IGNORE NULLS OVER (PARTITION BY category, supplier_id ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_least_stock_qty_price,
        CUME_DIST() OVER (PARTITION BY category ORDER BY least_stock_qty_price) AS least_stock_qty_price_cume_dist,
        STDDEV_SAMP(cost) OVER (PARTITION BY category) AS category_stddev_samp_cost,
        LAG(cost, 1) OVER (ORDER BY launch_date, id) AS prev_cost,
        DENSE_RANK() OVER (ORDER BY cost DESC) AS cost_dense_rank
    FROM valid
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        launch_date,
        least_stock_qty_price_cume_dist,
        cost_dense_rank,
        AVG(last_known_least_stock_qty_price) OVER (PARTITION BY DATE_TRUNC('month', launch_date) ORDER BY last_known_least_stock_qty_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last_known_least_stock_qty_price_avg_3,
        CUME_DIST() OVER (ORDER BY prev_cost NULLS LAST) AS prev_cost_cume_dist
    FROM scored
)
SELECT
    id,
    launch_date,
    least_stock_qty_price_cume_dist,
    cost_dense_rank,
    last_known_least_stock_qty_price_avg_3,
    prev_cost_cume_dist
FROM enriched;
