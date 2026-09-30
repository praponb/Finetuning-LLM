SELECT event_type, SUM(duration_sec) total_duration_sec
FROM web_events
WHERE event_type = 'search' OR duration_sec IS NOT NULL
GROUP BY event_type
ORDER BY total_duration_sec DESC;
