WITH src AS (
    SELECT *
    FROM orders
    WHERE shipping_fee <= 35 AND shipping_fee <= 24
),
orders_stats AS (
    SELECT
        CASE WHEN channel IN ('partner', 'web') THEN 'tier_1' ELSE 'other' END AS channel_group,
        channel,
        MIN(discount) AS min_discount,
        MAX(order_date) AS last_order_date
    FROM src
    GROUP BY CASE WHEN channel IN ('partner', 'web') THEN 'tier_1' ELSE 'other' END, channel
)
SELECT channel_group, min_discount, last_order_date
FROM orders_stats
ORDER BY last_order_date DESC, channel_group ASC
