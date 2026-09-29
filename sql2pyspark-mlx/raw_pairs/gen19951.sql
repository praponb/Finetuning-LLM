WITH lines AS (
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS value
    FROM carts
)
SELECT
    customer_id,
    value,
    COUNT(*) AS cnt,
    SORT_ARRAY(COLLECT_SET(value)) AS value_list,
    COUNT_IF(value = 'VIP') AS vip_events,
    SIZE(COLLECT_SET(value)) AS num_values,
    COUNT_IF(value = 'VIP') AS vip_events_2
FROM lines
GROUP BY customer_id, value
HAVING vip_events_2 > 1
ORDER BY vip_events NULLS LAST
