WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(brand) AS brand_lower
    FROM products
    WHERE launch_date >= TO_DATE('2023-11-05')
      AND YEAR(launch_date) = 2023
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_lower,
        FIRST_VALUE(cost, TRUE) OVER w_part AS first_known_cost,
        SUM(price) OVER w_part AS running_price,
        DENSE_RANK() OVER (ORDER BY stock_qty DESC NULLS LAST) AS stock_qty_dense_rank
    FROM valid
    WINDOW w_part AS (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY category, brand ORDER BY running_price, id) AS running_price_bucket,
        CUME_DIST() OVER (PARTITION BY supplier_id ORDER BY cost DESC) AS cost_cume_dist,
        LEAD(cost, 1) OVER (ORDER BY cost, id) AS next_cost
    FROM with_windows
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_lower,
        running_price,
        stock_qty_dense_rank,
        running_price_bucket,
        cost_cume_dist,
        next_cost,
        ARRAY_CONTAINS(tags, 'bestseller') AS is_bestseller,
        COALESCE(brand, 'OTHER') AS brand_filled
    FROM enriched
    WHERE launch_date > DATE '2023-12-08'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_lower,
        running_price,
        stock_qty_dense_rank,
        running_price_bucket,
        cost_cume_dist,
        next_cost,
        is_bestseller,
        brand_filled,
        LAG(stock_qty_dense_rank, 1) OVER (PARTITION BY supplier_id ORDER BY cost_cume_dist, id) AS prev_stock_qty_dense_rank,
        SUM(price) OVER (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price,
        SUM(next_cost) OVER (PARTITION BY category ORDER BY running_price_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_next_cost
    FROM base
),
valid_products AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(stock_qty_dense_rank AS STRING) AS stock_qty_dense_rank_str
    FROM scored
    WHERE supplier_id IS NOT NULL
      AND running_next_cost <= 1556
),
with_windows_products AS (
    -- Running and moving aggregates
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        brand_lower,
        running_price,
        stock_qty_dense_rank,
        running_price_bucket,
        cost_cume_dist,
        next_cost,
        is_bestseller,
        brand_filled,
        prev_stock_qty_dense_rank,
        cumulative_price,
        running_next_cost,
        stock_qty_dense_rank_str,
        SUM(stock_qty) OVER (ORDER BY cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty,
        PERCENT_RANK() OVER (ORDER BY prev_stock_qty_dense_rank DESC NULLS FIRST) AS prev_stock_qty_dense_rank_percent_rank,
        SUM(stock_qty_dense_rank) OVER w_part AS supplier_id_sum_stock_qty_dense_rank
    FROM valid_products
    WINDOW w_part AS (PARTITION BY supplier_id)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        supplier_id,
        category,
        cost
    FROM with_windows_products
) src
PIVOT (
    AVG(cost)
    FOR category IN ('Electronics' AS electronics, 'Fashion' AS fashion, 'Home' AS home)
);
