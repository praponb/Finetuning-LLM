SELECT *
FROM web_events
WHERE duration_sec <> 355
ORDER BY event_type DESC;
