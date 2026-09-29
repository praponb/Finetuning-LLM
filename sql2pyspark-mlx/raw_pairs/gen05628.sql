WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        stock_qty + price stock_qty_plus_price,
        COALESCE(brand, 'NONE') brand_filled
    FROM products
    WHERE supplier_id IS NOT NULL
      AND launch_date BETWEEN '2023-08-01' AND '2023-08-31'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.quantity > 72
        )
),
with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.brand,
        s.cost,
        s.stock_qty,
        s.launch_date,
        s.supplier_id,
        s.stock_qty_plus_price,
        s.brand_filled,
        latest.unit_price,
        latest.quantity
    FROM eligible s
    JOIN LATERAL (
            SELECT c.unit_price, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(quantity) OVER w_ordered running_avg_quantity,
        MIN(quantity) OVER w_ordered running_min_quantity,
        COUNT(quantity) OVER w_ordered running_count_quantity
    FROM with_latest
    WINDOW w_ordered AS (PARTITION BY brand ORDER BY cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(running_avg_quantity, 1) OVER w_hist prev_running_avg_quantity,
        CORR(cost, stock_qty) OVER (PARTITION BY brand) corr_cost_stock_qty,
        MAX(stock_qty) OVER (PARTITION BY brand, supplier_id) brand_max_stock_qty
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY YEAR(launch_date), supplier_id ORDER BY running_min_quantity ASC, id ASC)
),
eligible_products AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.units > 26
        )
)
SELECT
    s.id,
    s.brand,
    s.cost,
    s.stock_qty,
    s.launch_date,
    s.supplier_id,
    s.brand_filled,
    s.unit_price,
    s.quantity,
    (SELECT MAX(p2.price) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS max_product_price
FROM eligible_products s
ORDER BY quantity ASC, cost DESC;
