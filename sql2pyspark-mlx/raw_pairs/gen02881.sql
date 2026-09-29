WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        tag
    FROM carts
    LATERAL VIEW EXPLODE(coupon_codes) li AS tag
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN tag IN ('BUNDLE', 'WELCOME5') THEN 'Group A' ELSE 'Other' END AS tag_group,
        COALESCE(tag, 'OTHER') AS resolved_tag
    FROM flattened
    WHERE tag IS NOT NULL
),
joined AS (
    -- Attach customers attributes
    SELECT src.*, d.city
    FROM cleaned src
    LEFT JOIN customers d
        ON src.customer_id <=> d.id
),
cleaned_carts AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        created_at,
        tag,
        tag_group,
        city,
        CONCAT(city, '/', tag) AS city_tag
    FROM joined
    WHERE created_at > TO_TIMESTAMP('2025-03-01 20:00:00')
      AND created_at >= '2023-10-01'
),
totals AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('HOUR', created_at) AS hour_window,
        (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
        COUNT(*) AS cnt,
        (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS duration_min,
        SIZE(COLLECT_SET(tag)) AS num_tags,
        SORT_ARRAY(COLLECT_SET(tag)) AS tag_list,
        (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes_2
    FROM cleaned_carts
    GROUP BY ROLLUP (DATE_TRUNC('HOUR', created_at))
)
SELECT
    hour_window,
    span_minutes,
    cnt,
    duration_min,
    num_tags,
    tag_list,
    span_minutes_2
FROM totals
