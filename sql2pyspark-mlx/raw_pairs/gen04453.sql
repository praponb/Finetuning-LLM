WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, attr_value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT key, value
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) x AS value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(key) AS clean_key
    FROM lines
    WHERE key IN ('gift', 'utm_source', 'weight_kg')
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', key, value) AS key_value_key,
        COALESCE(value, 'Unknown') AS value_filled,
        COALESCE(key, 'OTHER') AS key_filled
    FROM filtered
    WHERE key IS NOT NULL
      AND value IN ('SPRING10', 'WELCOME5')
)
SELECT
    key,
    clean_key,
    key_value_key,
    value_filled,
    key_filled,
    CASE WHEN key RLIKE '^(device|weight_kg)$' THEN 'PRIORITY' WHEN key RLIKE '^(utm_source|gift)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS key_zone
FROM base
