WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS key, attr_value
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.key,
        a.attr_value,
        d.country,
        d.city
    FROM flattened a
    LEFT JOIN customers d
        ON a.customer_id = d.id
    WHERE d.is_active
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        key,
        attr_value,
        country,
        city,
        CASE WHEN key IN ('weight_kg', 'gift') THEN 'Priority' ELSE 'Other' END AS key_group,
        CONCAT(city, '/', key) AS city_key
    FROM enriched
    WHERE customer_id IS NOT NULL
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.key,
        src.attr_value,
        src.country,
        src.key_group,
        src.city_key,
        d.segment
    FROM base src
    INNER JOIN customers d
        ON src.customer_id = d.id
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        customer_id,
        segment,
        COUNT(CASE WHEN key = 'gift' THEN 1 END) AS gift_count,
        COUNT_IF(key = 'utm_source') AS utm_source_events,
        COUNT(CASE WHEN key = 'device' THEN 1 END) AS device_count,
        GROUPING(customer_id) AS gid
    FROM enriched_carts
    GROUP BY GROUPING SETS ((customer_id, segment), (customer_id))
)
SELECT gift_count, gid
FROM grouped
ORDER BY gift_count DESC
