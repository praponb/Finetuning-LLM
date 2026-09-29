SELECT YEAR(event_time) AS event_year, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, SUM(CASE WHEN page = '/search' THEN duration_sec ELSE 0 END) AS search_duration_sec
FROM web_events
WHERE duration_sec = 237
GROUP BY YEAR(event_time)
