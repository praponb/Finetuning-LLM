SELECT event_month, stddev_duration_sec, min_duration_sec
FROM (
    SELECT DATE_FORMAT(event_time, 'yyyy-MM') AS event_month, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, MIN(duration_sec) AS min_duration_sec
    FROM web_events
    GROUP BY DATE_FORMAT(event_time, 'yyyy-MM')
) AS sub
WHERE min_duration_sec > 410
ORDER BY stddev_duration_sec, event_month DESC;
