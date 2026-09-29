SELECT session_id, MIN(event_time) AS first_event_time
FROM web_events
WHERE duration_sec >= 249
GROUP BY session_id;
