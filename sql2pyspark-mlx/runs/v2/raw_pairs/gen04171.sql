SELECT id, duration_sec, page
FROM web_events
WHERE duration_sec <= (SELECT MIN(duration_sec) FROM web_events)
