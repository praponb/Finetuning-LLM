SELECT id, session_id, duration_sec, RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS rnk FROM web_events;
