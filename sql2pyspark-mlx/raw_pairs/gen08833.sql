WITH lines AS (
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
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        attr_value,
        value
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) x AS value
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN value IN ('SPRING10', 'BUNDLE') THEN 'Tier 1' ELSE 'Other' END AS value_group,
        CASE WHEN attr_key IN ('gift', 'utm_source') THEN 'Tier 1' ELSE 'Other' END AS attr_key_group
    FROM lines_carts
    WHERE value IN ('WELCOME5', 'SPRING10')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        attr_value,
        value,
        value_group,
        CASE WHEN attr_key IN ('weight_kg', 'device') THEN 'Core' ELSE 'Other' END AS attr_key_group_2,
        TRIM(value) AS clean_value,
        COALESCE(value, 'DIRECT') AS value_clean
    FROM base
    WHERE attr_key IS NOT NULL
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        attr_value,
        value,
        value_group,
        attr_key_group_2,
        value_clean,
        COALESCE(value, 'Unknown') AS value_filled
    FROM valid
    WHERE value = 'BUNDLE'
)
SELECT
    attr_value,
    value,
    value_group,
    attr_key_group_2,
    value_clean,
    value_filled
FROM base_carts
