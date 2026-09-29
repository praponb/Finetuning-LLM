WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS key, v
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        key,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM flattened
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM lines
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE created_at >= '2025-06-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(key) AS key_length
    FROM kept_rows
    WHERE key = 'utm_source'
)
-- Grouped metrics
SELECT
    key,
    ROUND(SUM(key_length) / NULLIF(COUNT(CASE WHEN key = 'device' THEN 1 END), 0), 2) AS key_length_per_device,
    SORT_ARRAY(COLLECT_SET(key)) AS key_list,
    ROUND(SUM(key_length) / NULLIF(COUNT(CASE WHEN element = 'BUNDLE' THEN 1 END), 0), 2) AS key_length_per_bundle
FROM valid
GROUP BY key
HAVING ROUND(SUM(key_length) / NULLIF(COUNT(CASE WHEN element = 'BUNDLE' THEN 1 END), 0), 2) > 0
ORDER BY key DESC;
