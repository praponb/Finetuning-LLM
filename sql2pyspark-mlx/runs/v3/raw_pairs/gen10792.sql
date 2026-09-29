WITH src AS (
    SELECT *
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 12 AND 15 AND duration_sec < 205 AND page IN ('/product', '/checkout', '/search')
),
event_type_summary AS (
    SELECT event_type, COUNT(*) AS cnt
    FROM src
    GROUP BY event_type
)
SELECT *
FROM event_type_summary
WHERE cnt > 8
ORDER BY event_type
