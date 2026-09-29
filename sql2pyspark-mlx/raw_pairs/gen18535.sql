WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TO_DATE(event_time) event_time_date,
        COALESCE(page, 'UNKNOWN') page_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-05-08 00:00:00'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, session_id
    FROM cleaned
    INTERSECT ALL
    SELECT id, session_id
    FROM web_events
    WHERE page NOT IN ('/cart', '/checkout')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.id = k.id AND s.session_id = k.session_id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.session_id,
        s.event_type,
        s.event_time,
        s.page_filled,
        b.browser
    FROM reconciled s
    LEFT JOIN sessions b
        ON s.session_id = b.id
    WHERE b.started_at < '2024-01-01'
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT e.*, b.browser AS session_browser
    FROM joined e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.session_id,
        e.event_type,
        e.session_browser,
        s.device
    FROM joined_web_events e
    LEFT JOIN sessions s
        ON e.session_id = s.id
)
SELECT
    session_id,
    event_type,
    session_browser,
    device,
    CASE WHEN session_browser RLIKE '^(Edge|Firefox)$' THEN 'GROUP_A' WHEN session_browser RLIKE '^(Chrome|Safari)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS session_browser_zone
FROM with_dims
ORDER BY session_browser;
