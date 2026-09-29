WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS k, v
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        v,
        EXPLODE(coupon_codes) AS element
    FROM flattened
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        INITCAP(TRIM(element)) AS element_title,
        COALESCE(element, 'NONE') AS element_filled
    FROM flattened_carts
    WHERE k IS NOT NULL
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        k,
        v,
        element_title,
        element_filled,
        CONCAT(element, ' ', k) AS element_k,
        COALESCE(k, 'UNKNOWN') AS k_clean
    FROM filtered
    WHERE k = 'utm_source'
)
-- Multi-level aggregation
SELECT
    k,
    SORT_ARRAY(COLLECT_SET(k)) AS k_list,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN k = 'device' THEN 1 END) AS device_count
FROM cleaned
GROUP BY k;
