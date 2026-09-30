WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        TRIM(country) clean_country,
        CONCAT(device, ' ', browser) device_browser,
        COALESCE(country, 'OTHER') country_clean
    FROM sessions
    WHERE started_at >= TIMESTAMP '2023-03-17 07:00:00'
      AND country IS NOT DISTINCT FROM 'DE'
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        x.web_event_event_type,
        x.page
    FROM base s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type, c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
base_sessions AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN web_event_event_type IN ('purchase', 'page_view') THEN 'Group A' ELSE 'Other' END web_event_event_type_group
    FROM with_latest
    WHERE page IS NOT NULL
      AND page = '/cart'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.page = '/search'
        )
),
base_sessions_2 AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN web_event_event_type IN ('add_to_cart', 'purchase') THEN 'Priority' ELSE 'Other' END web_event_event_type_group_2
    FROM eligible
    WHERE customer_id IS NOT NULL
),
base_sessions_3 AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        web_event_event_type,
        page,
        web_event_event_type_group,
        web_event_event_type_group_2,
        LOWER(page) page_lower
    FROM base_sessions_2
    WHERE page = '/checkout'
),
eligible_sessions AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base_sessions_3 s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page IS NOT NULL
        )
)
SELECT
    id,
    web_event_event_type,
    page,
    web_event_event_type_group,
    web_event_event_type_group_2,
    page_lower
FROM eligible_sessions
ORDER BY id DESC NULLS FIRST, web_event_event_type DESC
