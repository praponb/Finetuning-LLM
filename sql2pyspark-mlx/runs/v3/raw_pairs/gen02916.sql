WITH src AS (
    SELECT *
    FROM web_events
    WHERE page = '/checkout'
),
web_events_stats AS (
    SELECT MONTH(event_time) event_month, AVG(duration_sec) avg_duration_sec, SUM(duration_sec) total_duration_sec
    FROM src
    GROUP BY MONTH(event_time)
)
SELECT event_month, avg_duration_sec, total_duration_sec
FROM web_events_stats
WHERE avg_duration_sec < 260
