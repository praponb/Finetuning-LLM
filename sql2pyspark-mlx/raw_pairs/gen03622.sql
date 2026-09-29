WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        TRIM(channel) AS clean_channel
    FROM orders
    WHERE order_date > DATE '2023-03-21'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.paid_at >= '2024-06-01'
        )
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.paid_at
    FROM eligible s
    LEFT JOIN LATERAL (
            SELECT c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.quantity <= 5
        )
)
SELECT
    id,
    store_id,
    order_date,
    status,
    channel,
    amount,
    discount,
    shipping_fee,
    clean_channel,
    CASE WHEN channel RLIKE '^(web|mobile)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS channel_zone
FROM kept;
