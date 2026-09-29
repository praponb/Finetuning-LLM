WITH with_window AS (
    SELECT
        src.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM orders src
    LEFT JOIN subscriptions sub
        ON src.customer_id = sub.customer_id
       AND src.order_date >= sub.start_date
       AND src.order_date < sub.end_date + INTERVAL 14 DAYS
),
with_latest AS (
    SELECT s.*, latest.quantity
    FROM with_window s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
ranked AS (
    SELECT *
    FROM with_latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id, plan ORDER BY shipping_fee DESC, id, subscription_id) = 1
)
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    channel,
    amount,
    discount,
    subscription_id,
    plan,
    CASE WHEN plan RLIKE '^(enterprise|premium)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS plan_zone
FROM ranked
ORDER BY order_date;
