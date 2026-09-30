SELECT id, session_id, CAST(duration_sec AS DOUBLE) AS duration_sec_num
FROM web_events
WHERE session_id IN (16, 32, 36);
