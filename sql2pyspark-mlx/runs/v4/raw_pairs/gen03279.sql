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
    LATERAL VIEW EXPLODE(attributes) e AS key, v
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        key,
        v,
        code
    FROM exploded
    LATERAL VIEW OUTER EXPLODE(coupon_codes) x AS code
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN key = 'gift' THEN 1 ELSE 0 END AS is_gift,
        COALESCE(key, 'DIRECT') AS key_clean
    FROM flattened
    WHERE created_at >= TIMESTAMP '2025-03-02 09:00:00'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM valid
    INTERSECT
    SELECT id
    FROM carts
    WHERE customer_id IS NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    created_at,
    code,
    CASE WHEN code RLIKE '^(WELCOME5|SPRING10)$' THEN 'PRIORITY' WHEN code RLIKE '^(VIP|BUNDLE)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS code_zone
FROM verified
ORDER BY id DESC NULLS FIRST, code_zone DESC;
