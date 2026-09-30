SELECT event_time, CEIL(duration_sec) AS duration_sec_whole
FROM web_events
WHERE NOT (event_time >= '2023-12-01' AND (event_time < '2023-03-01' OR session_id IN (35, 40)))
