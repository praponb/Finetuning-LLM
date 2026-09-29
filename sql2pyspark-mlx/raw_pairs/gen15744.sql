WITH joined AS (
    SELECT
        a.id,
        a.order_id,
        a.shipped_date,
        a.delivered_date,
        a.cost,
        a.status,
        d.shipping_fee,
        d.discount,
        d.channel
    FROM shipments a
    LEFT JOIN orders d
        ON a.order_id = d.id
)
SELECT
    id,
    shipped_date,
    delivered_date,
    status,
    shipping_fee,
    discount,
    channel,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'OTHER_ZONE' WHEN channel RLIKE '^(web|mobile)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS channel_zone
FROM joined
ORDER BY shipped_date NULLS FIRST, channel_zone
