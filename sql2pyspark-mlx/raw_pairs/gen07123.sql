WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        CASE WHEN page IN ('/home', '/product') THEN 'Priority' ELSE 'Other' END AS page_group,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
      AND event_type NOT IN ('page_view', 'purchase', 'search')
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.event_time,
        a.page_filled,
        b.browser,
        b.device
    FROM base a
    LEFT JOIN sessions b
        ON a.session_id = b.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT DISTINCT
    SELECT id
    FROM web_events
    WHERE event_time >= '2024-01-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id
),
base_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        session_id,
        event_time,
        device,
        TRIM(device) AS clean_device
    FROM kept_rows
    WHERE event_time > DATE '2023-06-21'
      AND device IS NOT NULL
),
joined AS (
    -- Attach sessions attributes
    SELECT
        a.session_id,
        a.event_time,
        a.device,
        a.clean_device,
        s.country,
        s.browser,
        s.device AS session_device
    FROM base_web_events a
    LEFT JOIN sessions s
        ON a.session_id = s.id
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        s.device AS session_device_2,
        s.browser AS session_browser,
        s.country AS session_country
    FROM joined e
    LEFT JOIN sessions s
        ON e.session_id = s.id
    WHERE s.country = 'US'
)
-- Grouped metrics
SELECT
    session_device_2,
    session_device,
    COUNT(*) AS event_count,
    COUNT(DISTINCT session_browser) AS distinct_session_browsers,
    COUNT(CASE WHEN session_browser = 'Safari' THEN 1 END) AS safari_count,
    GROUPING(session_device_2) AS grouping_level
FROM enriched_web_events
GROUP BY GROUPING SETS ((session_device_2, session_device), (session_device_2), ())
HAVING COUNT(CASE WHEN session_browser = 'Safari' THEN 1 END) > 1;
