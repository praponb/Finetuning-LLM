WITH top_web_events AS (SELECT *, RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) pos FROM web_events) SELECT id, session_id, duration_sec FROM top_web_events WHERE pos = 1;
