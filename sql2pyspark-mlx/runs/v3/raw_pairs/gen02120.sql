SELECT
    event_type,
    CASE WHEN duration_sec >= 403 THEN 'high' WHEN duration_sec >= 202 THEN 'mid' ELSE 'low' END AS duration_sec_bucket,
    ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec,
    SUM(duration_sec) AS total_duration_sec
FROM web_events
GROUP BY 1, 2
HAVING stddev_duration_sec >= 59
