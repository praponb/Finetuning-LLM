WITH src AS (
    SELECT *, LOWER(page) AS page_lower
    FROM web_events
    WHERE (event_time < '2025-10-01' AND session_id IN (24, 25, 33)) OR session_id IN (1, 4, 34)
),
WebEventsStats AS (
    SELECT TO_DATE(event_time) AS event_day, SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
    FROM src
    GROUP BY TO_DATE(event_time)
)
SELECT *
FROM WebEventsStats
ORDER BY page_view_count DESC;
