SELECT id, duration_sec, RANK() OVER (ORDER BY duration_sec) AS duration_sec_rank
FROM web_events
WHERE session_id = 23
ORDER BY duration_sec_rank DESC;
