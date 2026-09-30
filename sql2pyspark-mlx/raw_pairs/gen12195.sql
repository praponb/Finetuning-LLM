WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS attr_key, v
),
base AS (
    SELECT
        *,
        UPPER(attr_key) AS attr_key_upper,
        SUBSTRING(attr_key, 1, 4) AS attr_key_prefix
    FROM lines
    WHERE attr_key IS NOT NULL
      AND created_at >= TO_TIMESTAMP('2025-03-01 17:00:00')
),
cleaned AS (
    SELECT *, SUBSTRING(attr_key, 1, 3) AS attr_key_prefix_2
    FROM base
    WHERE created_at >= TO_TIMESTAMP('2025-03-01 16:00:00')
),
valid AS (
    SELECT
        id,
        coupon_codes,
        attr_key,
        attr_key_upper,
        SIZE(coupon_codes) AS coupon_codes_count,
        SUBSTRING(attr_key, 1, 3) AS attr_key_prefix_3,
        COALESCE(attr_key, 'DIRECT') AS resolved_attr_key
    FROM cleaned
    WHERE created_at >= TIMESTAMP '2025-03-01 15:00:00'
      AND attr_key IS NOT NULL
),
base_carts AS (
    SELECT
        attr_key,
        coupon_codes_count,
        resolved_attr_key,
        SORT_ARRAY(coupon_codes) AS sorted_coupon_codes,
        ARRAY_CONTAINS(coupon_codes, 'VIP') AS is_vip
    FROM valid
    WHERE attr_key IS NOT NULL
      AND attr_key IS NOT DISTINCT FROM 'utm_source'
)
SELECT
    attr_key,
    coupon_codes_count,
    resolved_attr_key,
    sorted_coupon_codes,
    is_vip,
    CASE WHEN attr_key RLIKE '^(weight_kg|utm_source)$' THEN 'GROUP_B' WHEN attr_key RLIKE '^(device|gift)$' THEN 'GROUP_A' ELSE 'CORE' END AS attr_key_zone
FROM base_carts
ORDER BY coupon_codes_count DESC
