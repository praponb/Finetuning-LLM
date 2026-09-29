WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        stock_qty - cost AS stock_qty_minus_cost,
        DATE_SUB(launch_date, 51) AS launch_date_minus_51d,
        COALESCE(brand, 'NONE') AS brand_filled
    FROM products
    WHERE launch_date >= DATE '2023-07-13'
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.order_item_unit_price
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        price,
        cost,
        launch_date,
        brand_filled,
        order_item_unit_price
    FROM with_last
    QUALIFY RANK() OVER (PARTITION BY category ORDER BY order_item_unit_price DESC NULLS FIRST) = 1
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM latest
    EXCEPT DISTINCT
    SELECT id
    FROM products
    WHERE launch_date >= '2025-04-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    id,
    price,
    cost,
    launch_date,
    brand_filled,
    order_item_unit_price
FROM verified
ORDER BY cost DESC NULLS LAST
