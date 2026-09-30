WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v
    FROM exploded
    QUALIFY ROW_NUMBER() OVER (PARTITION BY attr_key, customer_id ORDER BY created_at ASC, id ASC, attr_key ASC) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(attr_key) AS clean_attr_key,
        ARRAY_JOIN(coupon_codes, ', ') AS coupon_codes_text,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_filled
    FROM latest
    WHERE customer_id = 6
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.country,
        b.segment,
        b.email
    FROM filtered src
    INNER JOIN customers b
        ON src.customer_id <=> b.id
    WHERE b.city NOT IN ('Berlin', 'Singapore', 'Tokyo')
)
-- Aggregate with conditional metrics
SELECT
    segment,
    SIZE(COLLECT_SET(attr_key)) AS num_attr_keys,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    COUNT_IF(attr_key = 'gift') AS gift_events
FROM enriched
GROUP BY segment
HAVING COUNT_IF(attr_key = 'gift') >= 1;
