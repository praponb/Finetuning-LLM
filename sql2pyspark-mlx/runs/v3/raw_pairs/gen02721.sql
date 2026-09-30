SELECT event_type, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
FROM web_events
WHERE duration_sec <= 311 AND duration_sec = 443
GROUP BY event_type
