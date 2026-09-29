SELECT id, event_type, CEIL(duration_sec) AS duration_sec_whole
FROM web_events
WHERE duration_sec = 374 AND session_id IN (1, 12, 27);
