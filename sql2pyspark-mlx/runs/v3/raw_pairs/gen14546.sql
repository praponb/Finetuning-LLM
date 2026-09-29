SELECT *
FROM (
    SELECT HOUR(event_time) AS event_hour, SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count, COUNT(*) AS web_event_count
    FROM web_events
    WHERE session_id IN (4, 34)
    GROUP BY HOUR(event_time)
) AS x
WHERE purchase_count >= 68
ORDER BY event_hour DESC
