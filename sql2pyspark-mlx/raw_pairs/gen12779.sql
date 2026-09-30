WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        quantity,
        unit_price,
        CASE WHEN quantity >= 8 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_level,
        CASE WHEN unit_price >= 310 THEN 'High' WHEN unit_price >= 60 THEN 'Medium' ELSE 'Low' END AS unit_price_level
    FROM order_items
    WHERE product_id IS NOT NULL
),
joined AS (
    -- Attach orders attributes
    SELECT
        e.*,
        b.discount,
        b.channel
    FROM cleaned e
    LEFT JOIN orders b
        ON e.order_id = b.id
    WHERE b.shipping_fee > 34
),
with_dims AS (
    -- Attach products attributes
    SELECT
        e.*,
        d.category,
        d.brand
    FROM joined e
    LEFT JOIN products d
        ON e.product_id <=> d.id
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY quantity, id) AS rn
        FROM with_dims
    ) ranked
    WHERE rn = 1
)
SELECT
    id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    channel,
    category,
    rn,
    CASE WHEN channel RLIKE '^(partner|mobile)$' THEN 'EXTENDED' WHEN channel RLIKE '^(web|store)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS channel_zone
FROM latest
ORDER BY product_id DESC;
