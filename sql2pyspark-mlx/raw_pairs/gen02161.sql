WITH exploded AS (
    SELECT
        id,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
),
valid AS (
    SELECT
        *,
        INITCAP(TRIM(attr_key)) AS attr_key_title,
        LOWER(attr_key) AS attr_key_lower
    FROM exploded
    WHERE attr_key IS NOT NULL
      AND attr_key IS DISTINCT FROM 'utm_source'
),
valid_carts AS (
    SELECT
        *,
        TRIM(attr_key) AS clean_attr_key,
        CASE WHEN attr_key IN ('device', 'weight_kg') THEN 'Priority' ELSE 'Other' END AS attr_key_group
    FROM valid
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
filtered AS (
    SELECT
        id,
        coupon_codes,
        attr_key,
        attr_key_title,
        clean_attr_key,
        attr_key_group,
        TRIM(attr_key) AS clean_attr_key_2
    FROM valid_carts
    WHERE attr_key IS NULL
),
base AS (
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        COALESCE(attr_key, 'NONE') AS resolved_attr_key
    FROM filtered
    WHERE attr_key IS NOT NULL
      AND SIZE(coupon_codes) > 1
),
base_carts AS (
    SELECT
        id,
        coupon_codes,
        attr_key,
        clean_attr_key,
        attr_key_group,
        clean_attr_key_2,
        attr_key_upper,
        resolved_attr_key,
        SIZE(coupon_codes) AS coupon_codes_count,
        COALESCE(attr_key, 'N/A') AS attr_key_clean
    FROM base
    WHERE attr_key IS NOT NULL
      AND SIZE(coupon_codes) > 1
)
SELECT
    id,
    attr_key,
    clean_attr_key,
    attr_key_group,
    attr_key_upper,
    resolved_attr_key,
    attr_key_clean
FROM base_carts
ORDER BY attr_key DESC
