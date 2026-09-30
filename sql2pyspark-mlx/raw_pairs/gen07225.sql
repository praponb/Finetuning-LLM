WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        country,
        TRIM(browser) AS clean_browser,
        COALESCE(browser, 'N/A') AS resolved_browser
    FROM sessions
    WHERE country IN ('JP', 'US', 'UK')
      AND browser IS NOT NULL
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.country,
        a.clean_browser,
        a.resolved_browser,
        p.email,
        p.city
    FROM base a
    INNER JOIN customers p
        ON a.customer_id <=> p.id
    WHERE LENGTH(p.email) >= 11
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.clean_browser,
        e.resolved_browser,
        e.email,
        c.city AS customer_city
    FROM with_dims e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.web_event_event_type
    FROM with_dims_sessions s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
joined AS (
    -- Attach customers attributes
    SELECT a.*, d.country
    FROM with_last a
    INNER JOIN customers d
        ON a.customer_id = d.id
),
totals AS (
    -- Multi-level aggregation
    SELECT
        customer_city,
        COUNT(*) FILTER (WHERE country = 'SG') AS sg_rows,
        SORT_ARRAY(COLLECT_SET(customer_city)) AS customer_city_list,
        COUNT(*) AS row_count,
        COUNT(*) AS cnt,
        SORT_ARRAY(COLLECT_SET(web_event_event_type)) AS web_event_event_type_list,
        COUNT(DISTINCT customer_city) AS distinct_customer_cities
    FROM joined
    GROUP BY 1
)
SELECT
    customer_city,
    sg_rows,
    customer_city_list,
    row_count,
    web_event_event_type_list,
    distinct_customer_cities,
    CASE WHEN customer_city RLIKE '^(Chiang Mai|Bangkok)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS customer_city_zone
FROM totals
