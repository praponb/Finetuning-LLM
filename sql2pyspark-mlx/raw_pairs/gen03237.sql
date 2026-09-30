WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, v
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        key,
        v,
        EXPLODE_OUTER(coupon_codes) AS value
    FROM exploded
),
with_dims AS (
    -- Attach customers attributes
    SELECT s.*, c.email
    FROM flattened s
    LEFT JOIN customers c
        ON s.customer_id = c.id
    WHERE NOT c.is_active
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CONCAT_WS('-', key, email) key_email_key
    FROM with_dims
    WHERE created_at > TIMESTAMP '2025-03-01 17:00:00'
      AND key IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        key,
        v,
        email,
        key_email_key,
        LOWER(key) key_lower
    FROM filtered
    WHERE created_at > TIMESTAMP '2025-03-01 22:00:00'
      AND key IN ('weight_kg', 'gift', 'utm_source')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, REPLACE(email, ' ', '') email_slug
    FROM cleaned
    WHERE created_at > DATE '2025-03-01'
      AND key IS NOT DISTINCT FROM 'weight_kg'
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        WEEKOFYEAR(created_at) created_at_weekofyear,
        COALESCE(key, 'UNKNOWN') key_clean
    FROM valid
    WHERE LENGTH(email) > 6
)
-- Grouped metrics
SELECT
    CASE WHEN created_at_weekofyear >= 64 THEN 'HIGH' ELSE 'LOW' END AS created_at_weekofyear_tier,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    COUNT_IF(key = 'device') AS device_events,
    COUNT(CASE WHEN key = 'device' THEN 1 END) AS device_count,
    SUM(created_at_weekofyear) FILTER (WHERE key = 'utm_source') AS utm_source_total_created_at_weekofyear,
    MIN(created_at) AS first_created_at,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list
FROM cleaned_carts
GROUP BY CASE WHEN created_at_weekofyear >= 64 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('DAY', created_at)
ORDER BY device_count DESC NULLS FIRST;
