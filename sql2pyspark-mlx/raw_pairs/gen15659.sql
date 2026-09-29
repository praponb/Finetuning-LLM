WITH base AS (
    SELECT
        id,
        store_id,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        COALESCE(channel, 'N/A') AS channel_filled,
        CONCAT_WS('-', status, channel) AS status_channel_key,
        COALESCE(channel, 'UNKNOWN') AS channel_filled_2
    FROM orders
    WHERE order_date > DATE '2023-09-17'
),
latest_match AS (
    SELECT
        s.*,
        latest.order_item_quantity,
        latest.unit_price
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    SELECT
        s.id,
        s.channel,
        s.amount,
        s.discount,
        s.channel_filled,
        s.channel_filled_2,
        s.order_item_quantity,
        s.unit_price,
        latest.payment_paid_at
    FROM latest_match s
    LEFT JOIN LATERAL (
            SELECT c.paid_at AS payment_paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY payment_paid_at, id) AS rank_in_group
    FROM with_latest
    QUALIFY rank_in_group = 1
),
metrics AS (
    SELECT
        id,
        channel,
        discount,
        channel_filled,
        unit_price,
        rank_in_group,
        AVG(discount) OVER (PARTITION BY channel ORDER BY rank_in_group, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS discount_avg_3,
        LAG(amount, 1, 0) OVER (PARTITION BY channel ORDER BY unit_price, id) AS prev_amount,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY unit_price NULLS LAST) AS unit_price_dense_rank
    FROM latest
)
SELECT
    CASE WHEN unit_price_dense_rank >= 6 THEN 'LARGE' ELSE 'SMALL' END AS unit_price_dense_rank_tier,
    COUNT_IF(channel = 'mobile') AS mobile_events,
    COUNT_IF(channel = 'mobile') AS mobile_events_2,
    MAX(unit_price_dense_rank) FILTER (WHERE channel = 'store') AS store_max_unit_price_dense_rank
FROM metrics
GROUP BY unit_price_dense_rank_tier;
