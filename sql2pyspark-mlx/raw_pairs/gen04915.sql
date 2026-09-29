WITH Src AS (
    SELECT *
    FROM web_events
    WHERE event_time < '2025-04-01'
),
page_summary AS (
    SELECT page, MAX(duration_sec) AS max_duration_sec, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values
    FROM Src
    GROUP BY page
)
SELECT *
FROM page_summary
ORDER BY event_type_values DESC
