WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS key, attr_value
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        key,
        attr_value,
        EXPLODE(coupon_codes) AS element
    FROM lines
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(key, '/', element) key_element,
        COALESCE(key, 'N/A') key_filled
    FROM lines_carts
    WHERE element IS NOT NULL
      AND element IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        attr_value,
        element,
        key_element,
        TRIM(element) clean_element,
        COALESCE(element, 'DIRECT') element_filled
    FROM filtered
    WHERE element IS NOT NULL
      AND key NOT IN ('weight_kg', 'gift', 'device')
)
SELECT
    attr_value,
    element,
    key_element,
    clean_element,
    element_filled,
    CASE WHEN element RLIKE '^(SPRING10|VIP)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS element_zone
FROM valid
