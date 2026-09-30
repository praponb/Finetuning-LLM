WITH filtered AS (
    -- Keep recent, valid rows
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
        SUBSTRING(brand, 1, 2) AS brand_prefix,
        price - stock_qty AS price_minus_stock_qty,
        COALESCE(brand, 'UNKNOWN') AS resolved_brand
    FROM products
    WHERE supplier_id IS NOT NULL
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        brand,
        price,
        stock_qty,
        launch_date,
        tags,
        brand_prefix,
        price_minus_stock_qty,
        MIN(price) OVER w_hist AS running_min_price,
        COUNT(price) OVER w_hist AS running_count_price,
        MAX(price) OVER w_hist AS running_max_price,
        AVG(price) OVER w_hist AS running_avg_price
    FROM filtered
    WINDOW w_hist AS (PARTITION BY category ORDER BY price_minus_stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock >= s.launch_date
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        brand,
        price,
        stock_qty,
        launch_date,
        brand_prefix,
        price_minus_stock_qty,
        running_count_price,
        running_max_price,
        LAST_DAY(launch_date) AS launch_date_month_end
    FROM kept
    WHERE stock_qty <= 138
),
kept_products AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.units < 24
        )
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', launch_date_month_end) AS year_start,
    DATE_TRUNC('MONTH', launch_date_month_end) AS activity_month,
    SIZE(COLLECT_SET(brand)) AS num_brands,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    PERCENTILE_APPROX(running_count_price, ARRAY(0.25, 0.5, 0.75)) AS running_count_price_quartiles,
    MAX(price) AS max_price,
    BIT_OR(stock_qty) AS stock_qty_bit_or,
    SUM(CASE WHEN brand = 'Globex' THEN stock_qty ELSE 0 END) AS globex_stock_qty
FROM kept_products
GROUP BY GROUPING SETS ((DATE_TRUNC('YEAR', launch_date_month_end), DATE_TRUNC('MONTH', launch_date_month_end)), (DATE_TRUNC('YEAR', launch_date_month_end)), ())
ORDER BY activity_month DESC NULLS FIRST
