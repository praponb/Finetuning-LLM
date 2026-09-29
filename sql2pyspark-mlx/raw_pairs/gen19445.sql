WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        created_at,
        k,
        value,
        EXPLODE(coupon_codes) AS code
    FROM exploded
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(k) AS k_upper,
        COALESCE(code, 'N/A') AS code_filled
    FROM lines
    WHERE created_at > TIMESTAMP '2025-03-01 11:00:00'
      AND k != 'device'
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        created_at,
        k,
        code,
        k_upper,
        code_filled,
        LOWER(code) AS code_lower,
        COALESCE(k, 'DIRECT') AS k_filled
    FROM cleaned
    WHERE created_at > TIMESTAMP '2025-03-01 12:00:00'
)
-- Multi-level aggregation
SELECT
    k,
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    COUNT_IF(code = 'BUNDLE') AS bundle_events,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list
FROM cleaned_carts
GROUP BY ROLLUP (k, DATE_TRUNC('HOUR', created_at), DATE_TRUNC('DAY', created_at))
ORDER BY k DESC NULLS FIRST
