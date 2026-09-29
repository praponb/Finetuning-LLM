SELECT event_type, COUNT(DISTINCT duration_sec) AS unique_sessions, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
GROUP BY event_type
HAVING unique_sessions >= 4
ORDER BY unique_sessions ASC, event_type ASC;
