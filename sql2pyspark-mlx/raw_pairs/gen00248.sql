WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        SUBSTRING(page, 1, 3) AS page_prefix
    FROM web_events
    WHERE event_time >= DATE '2023-05-11'
      AND page IS NOT NULL
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        event_type,
        page,
        page_prefix
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY event_time DESC, id ASC) AS dedup_rank
        FROM cleaned
    ) x
    WHERE dedup_rank = 1
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.page,
        e.page_prefix,
        p.device,
        p.browser
    FROM deduped e
    JOIN sessions p
        ON e.session_id = p.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(page) AS page_lower
    FROM with_dims
    WHERE page = '/checkout'
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(page, '/', browser) AS page_browser
    FROM base
    WHERE browser IS NOT NULL
      AND session_id = 2
)
-- Multi-level aggregation
SELECT
    device,
    session_id,
    browser,
    COUNT(*) AS num_rows,
    SIZE(COLLECT_SET(page)) AS num_pages
FROM valid
GROUP BY GROUPING SETS ((device, session_id, browser), (device, session_id), (device), ())
