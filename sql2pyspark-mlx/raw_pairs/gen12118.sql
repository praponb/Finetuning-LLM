WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, value
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        value,
        value_2
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) li AS value_2
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM flattened
    EXCEPT ALL
    SELECT id
    FROM carts
    WHERE customer_id IN (21, 35, 36)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, UPPER(value_2) AS value_2_upper
    FROM reconciled
    WHERE attr_key IS NOT NULL
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(value_2, 'N/A') AS value_2_filled
    FROM base
    WHERE attr_key != 'gift'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM cleaned
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE TO_DATE(created_at) >= '2025-09-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN kept_keys k ON s.id = k.id
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT id
    FROM kept_rows
    EXCEPT
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys_2 k ON s.id = k.id
)
-- Grouped metrics
SELECT
    attr_key,
    COUNT(*) FILTER (WHERE attr_key = 'weight_kg') AS weight_kg_rows,
    SIZE(COLLECT_SET(value_2)) AS num_value_2s,
    COUNT(*) FILTER (WHERE value_2 = 'WELCOME5') AS welcome5_rows,
    COUNT(*) FILTER (WHERE value_2 = 'BUNDLE') AS bundle_rows,
    SORT_ARRAY(COLLECT_SET(attr_key)) AS attr_key_list
FROM verified
GROUP BY attr_key
ORDER BY welcome5_rows NULLS LAST
