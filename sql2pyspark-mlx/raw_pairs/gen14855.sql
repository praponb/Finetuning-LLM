WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        element
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(created_at, 'yyyy-MM-dd') AS created_at_label,
        COALESCE(element, 'UNKNOWN') AS resolved_element
    FROM lines
    WHERE created_at >= TIMESTAMP '2025-03-02 02:00:00'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        element,
        TRIM(element) AS clean_element,
        CASE WHEN element = 'VIP' THEN 1 ELSE 0 END AS is_vip
    FROM filtered
    WHERE created_at >= TO_DATE('2025-03-01')
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        created_at,
        clean_element,
        CASE WHEN element IN ('VIP', 'SPRING10') THEN 'Tier 1' ELSE 'Other' END AS element_group,
        DATE_FORMAT(created_at, 'yyyy-MM-dd') AS created_at_label,
        COALESCE(element, 'OTHER') AS resolved_element
    FROM cleaned
    WHERE element IS NOT NULL
      AND HOUR(created_at) BETWEEN 10 AND 16
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    MAX(created_at) AS last_created_at,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM cleaned_carts
GROUP BY ROLLUP (DATE_TRUNC('HOUR', created_at))
