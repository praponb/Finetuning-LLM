SELECT event_type, cnt, first_event_time
FROM (
    SELECT event_type, COUNT(*) AS cnt, MIN(event_time) AS first_event_time
    FROM web_events
    GROUP BY event_type
) AS x
WHERE cnt < 6;
