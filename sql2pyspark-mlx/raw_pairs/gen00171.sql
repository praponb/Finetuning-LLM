WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        LOWER(page) AS page_lower,
        DATE_TRUNC('week', event_time) AS event_time_period,
        COALESCE(page, 'NONE') AS page_clean
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-06-23 12:00:00'
      AND page IS NOT NULL
)
-- Attach sessions attributes
SELECT
    src.id,
    src.session_id,
    src.page,
    src.event_time,
    src.page_lower,
    src.event_time_period,
    src.page_clean,
    s.browser
FROM valid src
INNER JOIN sessions s
    ON src.session_id = s.id;
