WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) t AS element
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(element) AS element_upper,
        SUBSTRING(element, 1, 3) AS element_prefix
    FROM flattened
    WHERE element IS NOT NULL
      AND element IN ('SPRING10', 'BUNDLE')
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN element IN ('VIP', 'SPRING10') THEN 'Core' ELSE 'Other' END AS element_group
    FROM base
    WHERE element = 'SPRING10'
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        element,
        element_upper,
        element_prefix,
        element_group,
        INITCAP(TRIM(element)) AS element_title,
        LOWER(element) AS element_lower
    FROM valid
    WHERE element IS NOT NULL
      AND element IS DISTINCT FROM 'BUNDLE'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        element,
        SIZE(COLLECT_SET(element)) AS num_elements,
        COUNT(DISTINCT element) AS distinct_elements,
        COUNT(*) AS event_count,
        COUNT(*) FILTER (WHERE element = 'VIP') AS vip_rows,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE element = 'SPRING10') AS spring10_rows
    FROM base_carts
    GROUP BY 1
)
SELECT
    element,
    num_elements,
    distinct_elements,
    event_count,
    vip_rows,
    distinct_ids
FROM summary;
