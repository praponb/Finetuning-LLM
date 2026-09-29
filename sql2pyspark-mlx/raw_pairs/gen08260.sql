WITH base AS (
    SELECT *
    FROM web_events
    WHERE session_id IN (9, 11, 20)
),
web_events_stats AS (
    SELECT event_type, ROUND(AVG(duration_sec), 2) avg_duration_sec, SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END) checkout_count
    FROM base
    GROUP BY event_type
)
SELECT *
FROM web_events_stats
WHERE avg_duration_sec < 41
ORDER BY checkout_count DESC, event_type
LIMIT 5
