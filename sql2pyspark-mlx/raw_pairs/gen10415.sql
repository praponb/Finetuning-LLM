SELECT event_type, COUNT(DISTINCT duration_sec) AS unique_pages, MAX(event_time) AS latest_event_time
FROM web_events
GROUP BY event_type
HAVING unique_pages >= 5
ORDER BY unique_pages
