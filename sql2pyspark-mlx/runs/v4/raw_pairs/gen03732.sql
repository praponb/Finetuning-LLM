WITH remaining_keys AS (
    SELECT event_type, page
    FROM web_events
    INTERSECT ALL
    SELECT event_type, page
    FROM web_events
    WHERE page = '/search'
),
verified AS (
    SELECT s.*
    FROM web_events s
    JOIN remaining_keys k ON s.event_type = k.event_type AND s.page = k.page
)
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    event_type,
    MAX(event_time) AS last_event_time,
    MAX(event_time) AS ended_at,
    COUNT_IF(event_type = 'search') AS search_events
FROM verified
GROUP BY week_start, event_type
ORDER BY week_start DESC
