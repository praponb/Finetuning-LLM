WITH filtered AS (
    SELECT *, CASE WHEN quantity >= 9 THEN 'High' WHEN quantity >= 3 THEN 'Medium' ELSE 'Low' END AS quantity_band
    FROM order_items
    WHERE unit_price IS NOT NULL
),
joined AS (
    SELECT
        src.id,
        src.order_id,
        src.unit_price,
        src.quantity_band,
        o.shipping_fee,
        o.discount
    FROM filtered src
    LEFT JOIN orders o
        ON src.order_id = o.id
    WHERE o.customer_id IS NULL
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY order_id ORDER BY unit_price DESC) AS latest_rank
    FROM joined
    QUALIFY latest_rank = 1
),
with_dims AS (
    SELECT
        src.id,
        src.order_id,
        src.unit_price,
        src.quantity_band,
        src.shipping_fee,
        src.discount,
        p.shipping_fee AS order_shipping_fee,
        p.channel
    FROM latest src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, order_id ORDER BY unit_price DESC, id) AS rn
        FROM with_dims
    ) x
    WHERE rn = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, order_id ORDER BY unit_price NULLS FIRST, id) AS rank_in_group
        FROM deduped
    ) x
    WHERE rank_in_group = 1
),
joined_order_items AS (
    SELECT
        src.id,
        src.order_id,
        src.discount,
        src.channel,
        src.rn,
        src.rank_in_group,
        o.discount AS order_discount,
        o.channel AS order_channel
    FROM ranked src
    LEFT JOIN orders o
        ON src.order_id = o.id
)
SELECT
    id,
    rn,
    rank_in_group,
    order_discount,
    order_channel
FROM joined_order_items
