WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN duration_sec >= 521 THEN 'High' WHEN duration_sec >= 143 THEN 'Medium' ELSE 'Low' END AS duration_sec_level,
        TRIM(event_type) AS clean_event_type
    FROM web_events
    WHERE page = '/cart'
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.duration_sec_level,
        s.clean_event_type,
        p.device,
        p.country,
        p.browser
    FROM base s
    LEFT JOIN sessions p
        ON s.session_id = p.id
    WHERE p.customer_id IN (7, 17, 21, 24)
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        p.browser AS session_browser,
        p.device AS session_device,
        p.country AS session_country
    FROM with_dims e
    LEFT JOIN sessions p
        ON e.session_id = p.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.duration_sec_level,
        s.device,
        s.browser,
        s.session_device,
        s.session_country,
        b.country AS session_country_2,
        b.device AS session_device_2
    FROM enriched s
    LEFT JOIN sessions b
        ON s.session_id = b.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        duration_sec_level,
        session_country,
        CONCAT_WS('-', session_country, device) AS session_country_device_key,
        CONCAT(session_device, ' - ', device) AS session_device_device
    FROM joined
    WHERE device IS NOT NULL
),
with_dims_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_type,
        s.duration_sec_level,
        s.session_country,
        s.session_country_device_key,
        p2.browser
    FROM cleaned s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
)
-- Aggregate with conditional metrics
SELECT
    browser,
    COUNT_IF(session_country = 'DE') AS de_events,
    COUNT_IF(browser = 'Firefox') AS firefox_events,
    COUNT(*) FILTER (WHERE browser = 'Chrome') AS chrome_rows
FROM with_dims_web_events
GROUP BY browser
ORDER BY de_events DESC;
