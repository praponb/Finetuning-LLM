WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(page, 1, 4) AS page_prefix
    FROM web_events
    WHERE page IN ('/cart', '/search', '/home')
),
enriched AS (
    -- Attach sessions attributes
    SELECT src.*, b.device
    FROM valid src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT e.*, d.country
    FROM enriched e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
stats AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('WEEK', event_time) AS week_start,
        COUNT_IF(device = 'desktop') AS desktop_events,
        COUNT(DISTINCT page) AS distinct_pages
    FROM with_dims
    GROUP BY 1
)
SELECT
    week_start,
    desktop_events,
    distinct_pages
FROM stats;
