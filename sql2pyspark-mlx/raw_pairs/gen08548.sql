WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price * quantity AS line_total,
        CAST(quantity AS DOUBLE) AS quantity_str
    FROM order_items
    WHERE unit_price < 300
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(quantity) OVER (PARTITION BY order_id, product_id ORDER BY quantity_str, id) AS next_quantity,
        LAG(quantity) OVER (PARTITION BY order_id, product_id ORDER BY quantity_str, id) AS prev_quantity,
        LEAD(line_total) OVER (PARTITION BY order_id, product_id ORDER BY quantity_str, id) AS next_line_total,
        LAG(line_total) OVER (PARTITION BY order_id, product_id ORDER BY quantity_str, id) AS prev_line_total,
        ROW_NUMBER() OVER (PARTITION BY order_id, product_id ORDER BY quantity_str, id) AS event_seq
    FROM filtered
),
enriched AS (
    -- Attach products attributes
    SELECT
        s.id,
        s.product_id,
        s.quantity,
        s.line_total,
        s.quantity_str,
        s.prev_line_total,
        s.event_seq,
        b.brand,
        b.launch_date
    FROM metrics s
    LEFT JOIN products b
        ON s.product_id <=> b.id
    WHERE b.stock_qty > 353
),
enriched_order_items AS (
    -- Attach products attributes
    SELECT a.*, p.launch_date AS product_launch_date
    FROM enriched a
    LEFT JOIN products p
        ON a.product_id <=> p.id
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        quantity,
        event_seq,
        brand,
        ROW_NUMBER() OVER (PARTITION BY YEAR(launch_date), product_id ORDER BY launch_date DESC, id) AS row_seq,
        NTILE(5) OVER (PARTITION BY brand, product_id ORDER BY quantity, id) AS quantity_bucket
    FROM enriched_order_items
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY row_seq, id) = 1
)
SELECT
    id,
    brand,
    row_seq,
    CASE WHEN brand RLIKE '^(Globex|Initech)$' THEN 'OTHER_ZONE' WHEN brand RLIKE '^(Acme|Umbrella)$' THEN 'CORE' ELSE 'EXTENDED' END AS brand_zone
FROM latest
ORDER BY row_seq NULLS LAST;
