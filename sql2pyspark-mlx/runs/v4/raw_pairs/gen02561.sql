WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LOWER(event_type) AS event_type_lower,
        HOUR(event_time) AS event_time_hour,
        COALESCE(page, 'DIRECT') AS page_filled
    FROM web_events
    WHERE event_time >= TIMESTAMP '2023-11-05 16:00:00'
      AND page IS NOT NULL
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        s.country,
        s.browser,
        s.device
    FROM base src
    JOIN sessions s
        ON src.session_id = s.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT session_id, page
    FROM enriched
    EXCEPT
    SELECT session_id, page
    FROM web_events
    WHERE event_type NOT IN ('page_view', 'search', 'click')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN matched_keys k ON s.session_id = k.session_id AND s.page = k.page
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(page, ', ', browser) AS page_browser,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM verified
    WHERE TO_DATE(event_time) >= '2023-11-11'
)
SELECT
    id,
    session_id,
    page,
    event_time,
    duration_sec,
    event_type_lower,
    event_time_hour,
    browser,
    device
FROM cleaned
ORDER BY browser DESC
