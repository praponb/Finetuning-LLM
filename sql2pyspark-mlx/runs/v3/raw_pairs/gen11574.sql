SELECT event_type, avg_duration_sec, max_duration_sec
FROM (
    SELECT event_type, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, MAX(duration_sec) AS max_duration_sec
    FROM web_events
    GROUP BY event_type
) AS sub
WHERE max_duration_sec >= 480
ORDER BY avg_duration_sec DESC;
