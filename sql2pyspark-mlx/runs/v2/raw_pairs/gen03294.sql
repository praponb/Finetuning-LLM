WITH filtered_web_events AS (
    SELECT *, CONCAT(event_type, ' - ', page) AS event_type_page
    FROM web_events
    WHERE (session_id IN (16, 27, 28) AND event_type = 'purchase') OR duration_sec <> 149
),
summary AS (
    SELECT YEAR(event_time) AS event_year, SUM(duration_sec) AS total_duration_sec, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM filtered_web_events
    GROUP BY YEAR(event_time)
)
SELECT event_year, total_duration_sec, avg_duration_sec
FROM summary
WHERE total_duration_sec < 2300
ORDER BY avg_duration_sec DESC
