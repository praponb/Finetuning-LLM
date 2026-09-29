WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS('-', page, event_type) AS page_event_type_key,
        INITCAP(TRIM(page)) AS page_title,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM web_events
    WHERE page = '/search'
)
-- Join a dimension and aggregate
SELECT
    d.country,
    e.page,
    COUNT(*) AS rows_total,
    MAX(e.duration_sec) AS max_duration_sec
FROM cleaned e
LEFT JOIN sessions d ON e.session_id = d.id
GROUP BY d.country, e.page
