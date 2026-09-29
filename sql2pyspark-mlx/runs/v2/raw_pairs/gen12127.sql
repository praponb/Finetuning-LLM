SELECT id, event_time
FROM web_events
WHERE event_time < '2023-06-18' AND duration_sec = 183;
