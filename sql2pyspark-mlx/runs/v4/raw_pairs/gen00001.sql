WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS k, value
),
lines_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        element
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) t AS element
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, LOWER(k) AS k_lower
    FROM lines_carts
    WHERE k IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(element) AS element_lower
    FROM base
    WHERE k IS NOT NULL
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        k,
        element,
        SUBSTRING(k, 1, 2) AS k_prefix
    FROM valid
    WHERE element = 'SPRING10'
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN k IN ('device', 'gift') THEN 'Group A' ELSE 'Other' END AS k_group,
        COALESCE(k, 'UNKNOWN') AS k_filled
    FROM filtered
    WHERE k IS NOT NULL
),
base_carts_2 AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        k,
        element,
        k_prefix,
        k_group,
        k_filled,
        LOWER(k) AS k_lower
    FROM base_carts
    WHERE element IS NOT NULL
      AND k IN ('utm_source', 'weight_kg', 'gift')
),
stats AS (
    -- Grouped metrics
    SELECT
        k,
        element,
        COUNT(DISTINCT element) AS distinct_elements,
        COUNT_IF(element = 'BUNDLE') AS bundle_events,
        SORT_ARRAY(COLLECT_SET(element)) AS element_list,
        DENSE_RANK() OVER (PARTITION BY k ORDER BY COUNT_IF(element = 'BUNDLE') DESC) AS bundle_events_dense_rank
    FROM base_carts_2
    GROUP BY k, element
    HAVING distinct_elements < 1
)
SELECT
    k,
    element,
    distinct_elements,
    bundle_events_dense_rank,
    CASE WHEN k RLIKE '^(weight_kg|gift)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS k_zone
FROM stats;
