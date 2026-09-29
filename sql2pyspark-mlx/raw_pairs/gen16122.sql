SELECT id, duration_sec, session_id, CEIL(duration_sec) AS duration_sec_whole FROM web_events WHERE event_time < '2023-07-01' AND duration_sec BETWEEN 255 AND 325 ORDER BY duration_sec_whole DESC;
