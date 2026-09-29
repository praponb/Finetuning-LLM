WITH joined AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.channel,
        o.order_date,
        o.discount
    FROM payments a
    LEFT JOIN orders o
        ON a.order_id = o.id
)
SELECT
    id,
    method,
    amount,
    channel,
    discount,
    CASE WHEN channel RLIKE '^(partner|store)$' THEN 'OTHER_ZONE' WHEN channel RLIKE '^(web|mobile)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS channel_zone
FROM joined
