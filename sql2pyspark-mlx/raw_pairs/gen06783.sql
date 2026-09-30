WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, attr_value
)
SELECT
    attr_key,
    customer_id,
    MAX(created_at) AS last_seen,
    COUNT_IF(attr_key = 'utm_source') AS utm_source_events,
    MAX(created_at) AS last_created_at,
    COUNT_IF(attr_key = 'device') AS device_events
FROM exploded
GROUP BY 1, 2;
