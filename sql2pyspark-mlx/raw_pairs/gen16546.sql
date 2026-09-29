WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        channel,
        discount,
        shipping_fee,
        CONCAT_WS('-', status, channel) AS status_channel_key,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM orders
    WHERE discount <= 0.2
      AND channel IS DISTINCT FROM 'web'
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.shipment_cost
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_last_orders AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.discount,
        s.shipping_fee,
        s.resolved_channel,
        s.shipment_cost,
        x.delivered_date
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
              AND c.shipped_date <= s.order_date
            ORDER BY c.shipped_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY shipment_cost, id) AS rank_in_group
    FROM with_last_orders
    QUALIFY rank_in_group = 1
)
SELECT
    s.id,
    s.store_id,
    s.order_date,
    s.shipping_fee,
    s.shipment_cost,
    s.delivered_date,
    s.rank_in_group,
    (SELECT SUM(x.discount) FROM orders x WHERE x.store_id = s.store_id) AS sum_order_discount
FROM latest s;
