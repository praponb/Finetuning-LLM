WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS attr_key, v
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        v,
        EXPLODE(coupon_codes) AS value
    FROM lines
),
base AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(attr_key) AS attr_key_upper
    FROM lines_carts
    WHERE value IS NOT NULL
      AND attr_key NOT IN ('device', 'weight_kg', 'gift')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        attr_key,
        v,
        value,
        attr_key_upper,
        TRIM(value) AS clean_value
    FROM base
    WHERE attr_key != 'gift'
)
-- Multi-level aggregation
SELECT
    attr_key,
    value,
    COUNT(*) AS event_count,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list,
    COUNT(*) FILTER (WHERE value = 'VIP') AS vip_rows,
    COUNT(CASE WHEN attr_key = 'gift' THEN 1 END) AS gift_count
FROM cleaned
GROUP BY attr_key, value
ORDER BY event_count DESC
