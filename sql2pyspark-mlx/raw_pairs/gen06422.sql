WITH FilteredWebEvents AS (
    SELECT *, CAST(duration_sec AS DOUBLE) AS duration_sec_num
    FROM web_events
    WHERE session_id = 9 AND session_id = 30
),
summary AS (
    SELECT YEAR(event_time) AS event_year, HOUR(event_time) AS event_hour, SUM(CASE WHEN page = '/cart' THEN duration_sec_num ELSE 0 END) AS cart_duration_sec_num
    FROM FilteredWebEvents
    GROUP BY YEAR(event_time), HOUR(event_time)
)
SELECT event_year, event_hour
FROM summary
WHERE cart_duration_sec_num < 50
ORDER BY event_hour, event_year DESC;
