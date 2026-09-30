SELECT session_id, stddev_duration_sec, duration_sec_count
FROM (
    SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, COUNT(duration_sec) AS duration_sec_count
    FROM web_events
    WHERE duration_sec > 248
    GROUP BY session_id
) AS s
WHERE duration_sec_count < 8
ORDER BY stddev_duration_sec DESC
