WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        price,
        cost,
        stock_qty,
        launch_date,
        price - stock_qty AS price_minus_stock_qty,
        DATE_TRUNC('WEEK', launch_date) AS launch_date_period,
        COALESCE(brand, 'UNKNOWN') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-08-17'
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_item_unit_price,
        top1.quantity
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        price,
        stock_qty,
        price_minus_stock_qty,
        launch_date_period,
        brand_filled,
        MIN(quantity) OVER w_running AS running_min_quantity,
        AVG(quantity) OVER w_running AS running_avg_quantity,
        SUM(quantity) OVER w_running AS running_sum_quantity,
        COUNT(quantity) OVER w_running AS running_count_quantity
    FROM latest_match
    WHERE price >= 410
    WINDOW w_running AS (PARTITION BY category ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT
    SELECT id
    FROM products
    WHERE brand = 'Globex'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.product_id = s.id
              AND c.quantity >= 4
        )
)
SELECT
    id,
    price,
    stock_qty,
    launch_date_period,
    brand_filled,
    running_min_quantity,
    running_avg_quantity,
    running_sum_quantity,
    running_count_quantity
FROM kept
ORDER BY launch_date_period DESC NULLS LAST
