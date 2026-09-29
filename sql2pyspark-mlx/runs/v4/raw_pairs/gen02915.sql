SELECT id, event_time, ROUND(duration_sec * 1.07, 2) AS adjusted_duration_sec
FROM web_events
WHERE duration_sec >= 226 AND page IS NOT NULL
