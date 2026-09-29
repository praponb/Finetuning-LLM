WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.customer_id,
        e.created_at,
        p.name
    FROM flattened e
    INNER JOIN customers p
        ON e.customer_id <=> p.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, TO_DATE(created_at) AS created_at_date
    FROM joined
    WHERE TO_DATE(created_at) >= '2024-12-01'
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        src.*,
        d.email,
        d.city
    FROM filtered src
    LEFT JOIN customers d
        ON src.customer_id = d.id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', created_at_date) AS activity_month,
        COUNT_IF(city = 'Singapore') AS singapore_events,
        SIZE(COLLECT_SET(city)) AS num_cities,
        COUNT(*) AS row_count,
        GROUPING_ID() AS gid
    FROM with_dims
    GROUP BY CUBE (DATE_TRUNC('MONTH', created_at_date))
)
SELECT
    activity_month,
    num_cities,
    row_count,
    gid
FROM summary
ORDER BY row_count ASC NULLS FIRST;
