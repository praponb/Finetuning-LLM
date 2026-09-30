SELECT *
FROM (
    SELECT HOUR(event_time) AS event_hour, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    WHERE event_type = 'click'
    GROUP BY HOUR(event_time)
) AS sub
WHERE max_duration_sec > 310
ORDER BY max_duration_sec DESC;
