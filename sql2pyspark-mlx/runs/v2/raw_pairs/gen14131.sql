SELECT event_type, stddev_duration_sec, duration_sec_count
FROM (
    SELECT event_type, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    WHERE duration_sec > 248
    GROUP BY event_type
) AS s
WHERE duration_sec_count < 8
ORDER BY stddev_duration_sec DESC
