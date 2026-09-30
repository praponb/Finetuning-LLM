WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        FLOOR(quantity) AS quantity_whole,
        COALESCE(unit_price, 0) AS unit_price_filled
    FROM order_items
    WHERE product_id IS NOT NULL
      AND order_id IN (3, 23, 32, 39)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(quantity_whole) OVER (PARTITION BY order_id) AS order_id_avg_quantity_whole,
        FIRST_VALUE(quantity) IGNORE NULLS OVER (PARTITION BY order_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_quantity
    FROM filtered
),
metrics_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY product_id ORDER BY quantity, id) AS quantity_bucket,
        NTILE(10) OVER (ORDER BY unit_price DESC, id) AS unit_price_bucket,
        AVG(unit_price) OVER w_grp AS unit_price_avg_5,
        STDDEV_SAMP(first_known_quantity) OVER w_part AS product_id_stddev_samp_first_known_quantity
    FROM metrics
    WINDOW w_grp AS (PARTITION BY product_id ORDER BY quantity, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY product_id, order_id)
),
with_dims AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.order_id,
        s.product_id,
        s.quantity,
        s.unit_price,
        s.unit_price_filled,
        s.unit_price_bucket,
        s.unit_price_avg_5,
        s.product_id_stddev_samp_first_known_quantity,
        p.stock_qty,
        p.category,
        p.brand
    FROM metrics_order_items s
    LEFT JOIN products p
        ON s.product_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CEIL(product_id_stddev_samp_first_known_quantity) AS product_id_stddev_samp_first_known_quantity_whole,
        stock_qty + unit_price_avg_5 AS stock_qty_plus_unit_price_avg_5
    FROM with_dims
    WHERE unit_price_avg_5 >= 190
)
-- Join a dimension and aggregate
SELECT
    p.brand AS product_brand,
    s.brand AS order_item_brand,
    COUNT(*) AS cnt,
    MIN(s.product_id_stddev_samp_first_known_quantity) AS min_product_id_stddev_samp_first_known_quantity,
    GROUPING_ID() AS grouping_level
FROM base s
INNER JOIN products p ON s.product_id = p.id
GROUP BY CUBE (p.brand, s.brand)
ORDER BY min_product_id_stddev_samp_first_known_quantity DESC NULLS LAST;
