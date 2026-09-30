SELECT event_type, page, MAX(duration_sec) max_duration_sec
FROM web_events
WHERE page IS NOT NULL
GROUP BY event_type, page
HAVING MAX(duration_sec) < 380;
