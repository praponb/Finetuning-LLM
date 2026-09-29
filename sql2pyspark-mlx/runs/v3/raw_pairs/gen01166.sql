WITH filtered_web_events AS (
    SELECT *
    FROM web_events
    WHERE event_type <> 'purchase'
),
summary AS (
    SELECT event_type, COUNT(CASE WHEN event_type = 'page_view' THEN 1 END) AS page_view_count
    FROM filtered_web_events
    GROUP BY event_type
)
SELECT event_type, page_view_count
FROM summary
WHERE page_view_count > 8
ORDER BY page_view_count DESC, event_type
LIMIT 20
