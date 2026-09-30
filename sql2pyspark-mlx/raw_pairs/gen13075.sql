WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p2.browser,
        p2.device,
        p2.country
    FROM web_events s
    INNER JOIN sessions p2
        ON s.session_id = p2.id
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_dims
    QUALIFY ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST, id) = 1
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT e.*, p.browser AS session_browser
    FROM ranked e
    LEFT JOIN sessions p
        ON e.session_id = p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT event_type
    FROM with_dims_web_events
    EXCEPT DISTINCT
    SELECT event_type
    FROM web_events
    WHERE duration_sec BETWEEN 348 AND 366
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_dims_web_events s
    INNER JOIN kept_keys k ON s.event_type = k.event_type
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        browser,
        session_browser,
        TO_DATE(event_time) AS event_time_date,
        COALESCE(browser, 'UNKNOWN') AS browser_clean
    FROM verified
    WHERE event_time > TO_TIMESTAMP('2023-09-08 21:00:00')
      AND session_browser IS NOT NULL
),
joined AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        s.browser AS session_browser_2,
        s.country
    FROM base e
    INNER JOIN sessions s
        ON e.session_id = s.id
    WHERE s.device IN ('tablet', 'desktop')
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.session_id,
        a.event_type,
        a.browser,
        a.event_time_date,
        a.browser_clean,
        d.browser AS session_browser_3,
        d.country AS session_country,
        d.device
    FROM joined a
    INNER JOIN sessions d
        ON a.session_id = d.id
)
-- Multi-level aggregation
SELECT
    event_type,
    session_id,
    DATE_TRUNC('MONTH', event_time_date) AS activity_month,
    COUNT(CASE WHEN session_browser_3 = 'Chrome' THEN 1 END) AS chrome_count,
    COUNT_IF(event_type = 'click') AS click_events,
    COUNT_IF(session_country = 'UK') AS uk_events,
    COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_count,
    COUNT(CASE WHEN device = 'desktop' THEN 1 END) AS desktop_count
FROM enriched
GROUP BY 1, 2, 3;
