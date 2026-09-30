WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        COALESCE(price, 0) AS price_filled,
        CAST(price AS INT) AS price_int,
        COALESCE(brand, 'N/A') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-09-08'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(price_int) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) AS prev_price_int,
        LEAD(price_int) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) AS next_price_int,
        LAG(stock_qty) OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) AS prev_stock_qty,
        ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY stock_qty, id) AS seq_no
    FROM filtered
    WHERE category IN ('Sports', 'Books', 'Grocery')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        product_name,
        price,
        cost,
        stock_qty,
        launch_date,
        supplier_id,
        price_int,
        brand_filled,
        prev_price_int,
        next_price_int,
        prev_stock_qty,
        CASE WHEN price_int >= 62 THEN 'High' WHEN price_int >= 24 THEN 'Medium' ELSE 'Low' END AS price_int_band,
        price_filled + cost AS price_filled_plus_cost
    FROM enriched
    WHERE launch_date > TO_DATE('2023-06-17')
      AND supplier_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_name,
        price,
        cost,
        stock_qty,
        prev_price_int,
        prev_stock_qty,
        AVG(price_int) OVER (PARTITION BY supplier_id ORDER BY price_filled_plus_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS price_int_avg_3,
        SUM(stock_qty) OVER (PARTITION BY MONTH(launch_date) ORDER BY launch_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty,
        CUME_DIST() OVER (PARTITION BY supplier_id ORDER BY stock_qty NULLS FIRST) AS stock_qty_cume_dist,
        SUM(prev_stock_qty) OVER (ORDER BY price_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_prev_stock_qty
    FROM cleaned
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        prev_price_int,
        price_int_avg_3,
        running_stock_qty,
        running_prev_stock_qty,
        COUNT(prev_stock_qty) OVER w_running AS running_count_prev_stock_qty,
        MIN(prev_stock_qty) OVER w_running AS running_min_prev_stock_qty,
        MAX(prev_stock_qty) OVER w_running AS running_max_prev_stock_qty,
        SUM(stock_qty) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_stock_qty_2
    FROM metrics
    WINDOW w_running AS (ORDER BY price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        running_min_prev_stock_qty,
        running_max_prev_stock_qty
    FROM with_windows
) src
UNPIVOT (
    value FOR measure IN (running_min_prev_stock_qty AS RUNNING_MIN_PREV_STOCK_QTY, running_max_prev_stock_qty AS RUNNING_MAX_PREV_STOCK_QTY)
);
