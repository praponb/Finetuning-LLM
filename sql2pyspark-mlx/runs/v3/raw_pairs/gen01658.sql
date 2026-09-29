SELECT page FROM web_events WHERE session_id = 23 UNION ALL SELECT page FROM web_events WHERE duration_sec = 388 ORDER BY page;
