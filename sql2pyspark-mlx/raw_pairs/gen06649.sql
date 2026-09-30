SELECT event_type, SUM(duration_sec) AS total_duration_sec, RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS duration_sec_rank
FROM web_events
WHERE duration_sec < 248
GROUP BY event_type
ORDER BY event_type
