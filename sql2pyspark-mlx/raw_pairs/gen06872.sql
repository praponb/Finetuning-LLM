WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, attr_value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        attr_key,
        EXPLODE_OUTER(coupon_codes) AS code
    FROM exploded
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(created_at, 'yyyy-MM-dd') AS created_at_label,
        COALESCE(attr_key, 'N/A') AS attr_key_filled
    FROM flattened
    WHERE created_at > DATE '2025-03-01'
),
valid_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label_2,
        COALESCE(attr_key, 'NONE') AS attr_key_filled_2
    FROM valid
    WHERE attr_key <> 'utm_source'
)
SELECT
    id,
    created_at,
    attr_key,
    code,
    attr_key_filled,
    attr_key_filled_2
FROM valid_carts
