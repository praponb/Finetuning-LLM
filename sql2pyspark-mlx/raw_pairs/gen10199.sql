WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE order_id IN (34, 36)
),
joined AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.order_id,
        e.product_id,
        e.quantity,
        e.unit_price,
        e.greatest_quantity_unit_price,
        p.stock_qty,
        p.cost,
        p.category
    FROM cleaned e
    LEFT JOIN products p
        ON e.product_id = p.id
    WHERE p.category = 'Home'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        product_id,
        stock_qty,
        category,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY product_id), 4) AS unit_price_share,
        NTILE(4) OVER (PARTITION BY category, order_id ORDER BY unit_price DESC, id) AS unit_price_bucket,
        MIN(quantity) OVER (PARTITION BY order_id) AS order_id_min_quantity,
        CUME_DIST() OVER (PARTITION BY order_id ORDER BY greatest_quantity_unit_price DESC NULLS LAST) AS greatest_quantity_unit_price_cume_dist,
        ROUND(unit_price / SUM(unit_price) OVER (PARTITION BY order_id), 4) AS unit_price_share_2
    FROM joined
),
enriched AS (
    -- Attach products attributes
    SELECT
        src.*,
        p.launch_date,
        p.category AS product_category
    FROM with_windows src
    LEFT JOIN products p
        ON src.product_id = p.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_category, order_id ORDER BY stock_qty DESC NULLS FIRST, id) = 1
)
-- Ranking and gap features
SELECT
    id,
    product_id,
    stock_qty,
    unit_price_bucket,
    greatest_quantity_unit_price_cume_dist,
    unit_price_share_2,
    LAG(unit_price_bucket, 1) OVER (PARTITION BY category ORDER BY stock_qty, id) AS prev_unit_price_bucket,
    RANK() OVER (PARTITION BY order_id ORDER BY greatest_quantity_unit_price_cume_dist DESC NULLS LAST) AS greatest_quantity_unit_price_cume_dist_rank,
    AVG(unit_price_share) OVER (PARTITION BY category) AS category_avg_unit_price_share
FROM ranked
