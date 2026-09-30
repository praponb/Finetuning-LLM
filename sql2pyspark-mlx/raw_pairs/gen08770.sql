WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM flattened
    INTERSECT
    SELECT id
    FROM carts
    WHERE customer_id IN (22, 24, 35)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN remaining_keys k ON s.id = k.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(tag) AS clean_tag,
        COALESCE(tag, 'NONE') AS tag_clean
    FROM kept_rows
    WHERE created_at >= DATE '2025-03-01'
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        created_at,
        clean_tag,
        tag_clean,
        INITCAP(TRIM(tag)) AS tag_title,
        CASE WHEN tag IN ('SPRING10', 'VIP') THEN 'Priority' ELSE 'Other' END AS tag_group,
        COALESCE(tag, 'OTHER') AS tag_clean_2
    FROM valid
    WHERE created_at >= DATE '2025-03-01'
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATEDIFF(CURRENT_DATE(), created_at) / 30 AS months_since_created_at
    FROM base
    WHERE created_at < '2024-04-01'
)
SELECT
    id,
    created_at,
    tag_title,
    months_since_created_at
FROM base_carts
ORDER BY id DESC NULLS LAST, months_since_created_at
