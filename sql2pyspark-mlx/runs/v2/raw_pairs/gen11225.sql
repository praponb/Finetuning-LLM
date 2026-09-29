WITH valid_web_events AS (
    SELECT *
    FROM web_events
    WHERE session_id = 33 AND event_time >= '2025-10-18'
),
page_summary AS (
    SELECT
        page,
        event_type,
        COUNT(duration_sec) AS duration_sec_count,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM valid_web_events
    GROUP BY page, event_type
)
SELECT
    page,
    event_type,
    duration_sec_count,
    page_values
FROM page_summary
ORDER BY event_type, page
LIMIT 5
