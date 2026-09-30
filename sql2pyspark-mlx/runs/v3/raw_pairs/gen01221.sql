WITH src AS (
    SELECT *, INITCAP(TRIM(event_type)) event_type_title
    FROM web_events
    WHERE session_id IN (26, 29, 31, 33)
),
summary AS (
    SELECT YEAR(event_time) event_year, COUNT(CASE WHEN page = '/checkout' THEN 1 END) checkout_count
    FROM src
    GROUP BY YEAR(event_time)
)
SELECT event_year, checkout_count
FROM summary
ORDER BY event_year DESC, checkout_count DESC;
