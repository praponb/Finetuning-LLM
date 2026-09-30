WITH with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.product_name,
        s.price,
        s.stock_qty,
        s.launch_date,
        s.tags,
        top1.order_item_unit_price,
        top1.order_item_quantity
    FROM products s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_item_unit_price_2
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price_2
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
base AS (
    -- Keep recent, valid rows
    SELECT *, order_item_unit_price - order_item_quantity order_item_unit_price_minus_order_item_quantity
    FROM latest_match
    WHERE launch_date >= DATE '2023-08-20'
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        price,
        launch_date,
        tags,
        order_item_unit_price,
        order_item_quantity,
        order_item_unit_price_2,
        order_item_unit_price_minus_order_item_quantity,
        ROUND(price, 1) price_rounded
    FROM base
    WHERE launch_date >= TO_DATE('2023-09-04')
),
daily AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', launch_date) AS week_start,
        DATE_TRUNC('MONTH', launch_date) AS activity_month,
        DATE_TRUNC('QUARTER', launch_date) AS quarter_start,
        APPROX_PERCENTILE(order_item_unit_price_minus_order_item_quantity, ARRAY(0.25, 0.5, 0.75)) AS order_item_unit_price_minus_order_item_quantity_quartiles,
        COUNT(*) AS cnt,
        MAX(order_item_unit_price_minus_order_item_quantity) AS max_order_item_unit_price_minus_order_item_quantity,
        ROUND(AVG(order_item_unit_price_2), 2) AS avg_order_item_unit_price_2,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('WEEK', launch_date) ORDER BY ROUND(AVG(order_item_unit_price_2), 2) DESC) AS avg_order_item_unit_price_2_dense_rank
    FROM cleaned
    GROUP BY DATE_TRUNC('WEEK', launch_date), DATE_TRUNC('MONTH', launch_date), DATE_TRUNC('QUARTER', launch_date)
)
SELECT
    week_start,
    activity_month,
    quarter_start,
    max_order_item_unit_price_minus_order_item_quantity,
    avg_order_item_unit_price_2_dense_rank
FROM daily
ORDER BY avg_order_item_unit_price_2_dense_rank DESC NULLS FIRST, week_start ASC
