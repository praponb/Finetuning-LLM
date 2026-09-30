WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CEIL(quantity) AS quantity_whole,
        unit_price + quantity AS line_total
    FROM order_items
    WHERE unit_price <= 230
),
enriched AS (
    -- Attach products attributes
    SELECT src.*, p.launch_date
    FROM base src
    LEFT JOIN products p
        ON src.product_id = p.id
),
enriched_order_items AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY order_id ORDER BY quantity DESC, id ASC) AS quantity_bucket,
        SUM(quantity) OVER (PARTITION BY product_id ORDER BY line_total ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_quantity,
        AVG(unit_price) OVER (ORDER BY line_total ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS unit_price_avg_3
    FROM enriched
)
SELECT
    s.id,
    s.order_id,
    s.unit_price,
    s.quantity_whole,
    s.line_total,
    s.launch_date,
    s.quantity_bucket,
    s.cumulative_quantity,
    s.unit_price_avg_3,
    (SELECT MIN(o2.unit_price) FROM order_items o2 WHERE o2.order_id = s.order_id) AS min_order_item_unit_price
FROM enriched_order_items s
ORDER BY line_total DESC;
