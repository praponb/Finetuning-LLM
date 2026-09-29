SELECT ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec FROM web_events WHERE duration_sec BETWEEN 240 AND 397 ORDER BY stddev_duration_sec DESC
