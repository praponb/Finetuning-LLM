SELECT event_type, COUNT(*) AS cnt
FROM web_events
WHERE event_time >= '2025-01-01'
GROUP BY event_type
ORDER BY cnt DESC;
