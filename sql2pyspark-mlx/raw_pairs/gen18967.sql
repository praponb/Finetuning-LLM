WITH flattened AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS attr_key, value
),
flattened_carts AS (
    SELECT
        id,
        attr_key,
        value,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM flattened
),
base AS (
    SELECT
        *,
        INITCAP(TRIM(attr_key)) AS attr_key_title,
        COALESCE(element, 'Unknown') AS element_filled
    FROM flattened_carts
    WHERE attr_key NOT IN ('weight_kg', 'device')
),
filtered AS (
    SELECT
        id,
        attr_key,
        value,
        element,
        attr_key_title,
        element_filled,
        TRIM(attr_key) AS clean_attr_key
    FROM base
    WHERE attr_key IS NOT NULL
),
kept_keys AS (
    SELECT id
    FROM filtered
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'SPRING10')
),
reconciled AS (
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    attr_key,
    element,
    COUNT(DISTINCT id) AS distinct_ids,
    SORT_ARRAY(COLLECT_SET(element)) AS element_list,
    COUNT(*) FILTER (WHERE element = 'SPRING10') AS spring10_rows,
    COUNT(CASE WHEN element = 'VIP' THEN 1 END) AS vip_count,
    COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows,
    SORT_ARRAY(COLLECT_SET(element)) AS element_list_2,
    GROUPING(attr_key) AS grouping_id
FROM reconciled
GROUP BY GROUPING SETS ((attr_key, element), (attr_key), ())
HAVING spring10_rows >= 1
