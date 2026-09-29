WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS key, attr_value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        key,
        EXPLODE(coupon_codes) AS tag
    FROM exploded
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(tag)) AS tag_title,
        COALESCE(tag, 'OTHER') AS tag_clean
    FROM flattened
    WHERE tag IS NOT NULL
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(key, ', ', tag) AS key_tag,
        COALESCE(tag, 'UNKNOWN') AS tag_clean_2
    FROM cleaned
    WHERE tag IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        key,
        tag_title,
        tag_clean,
        key_tag,
        tag_clean_2,
        SUBSTRING(tag, 1, 3) AS tag_prefix,
        COALESCE(tag, 'N/A') AS tag_filled
    FROM filtered
    WHERE key = 'utm_source'
),
filtered_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(key, 1, 3) AS key_prefix,
        COALESCE(key, 'NONE') AS key_clean
    FROM base
    WHERE key IS NOT NULL
      AND key IN ('weight_kg', 'utm_source', 'gift')
),
filtered_carts_2 AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SUBSTRING(key, 1, 3) AS key_prefix_2,
        COALESCE(key, 'N/A') AS resolved_key
    FROM filtered_carts
    WHERE key = 'gift'
)
SELECT
    tag_clean,
    key_tag,
    tag_clean_2,
    tag_prefix,
    tag_filled,
    key_prefix,
    key_clean,
    key_prefix_2,
    resolved_key
FROM filtered_carts_2;
