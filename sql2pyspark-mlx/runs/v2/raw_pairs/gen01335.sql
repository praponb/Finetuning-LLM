SELECT event_type, SUM(duration_sec) AS total_duration_sec, RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS rnk
FROM web_events
GROUP BY event_type
ORDER BY rnk DESC;
