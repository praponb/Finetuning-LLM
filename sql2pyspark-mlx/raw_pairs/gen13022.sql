WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, attr_value
)
-- Grouped metrics
SELECT
    DATE_TRUNC('DAY', created_at) AS activity_day,
    customer_id,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
    SIZE(COLLECT_SET(k)) AS num_ks,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM exploded
GROUP BY DATE_TRUNC('DAY', created_at), customer_id, DATE_TRUNC('HOUR', created_at)
ORDER BY num_ks;
