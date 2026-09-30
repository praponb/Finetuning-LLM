SELECT YEAR(event_time) AS event_year, SUM(duration_sec) AS total_duration_sec
FROM web_events
GROUP BY YEAR(event_time)
ORDER BY event_year
LIMIT 5
