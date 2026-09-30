WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, value
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM lines
    EXCEPT DISTINCT
    SELECT id
    FROM carts
    WHERE SIZE(coupon_codes) >= 2
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        coupon_codes,
        k,
        HOUR(created_at) AS created_at_hour,
        LOWER(k) AS k_lower
    FROM verified
    WHERE created_at > TO_DATE('2025-03-01')
      AND k IS NOT DISTINCT FROM 'weight_kg'
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        k,
        k_lower,
        CAST(created_at_hour AS STRING) AS created_at_hour_num,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM base
    WHERE created_at >= DATE '2025-03-01'
      AND created_at >= '2025-01-01'
),
base_carts AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(k, 1, 4) AS k_prefix,
        COALESCE(k, 'NONE') AS resolved_k
    FROM cleaned
    WHERE k IN ('device', 'utm_source', 'weight_kg')
)
SELECT
    k_lower,
    created_at_hour_num,
    k_clean,
    k_prefix,
    resolved_k
FROM base_carts;
