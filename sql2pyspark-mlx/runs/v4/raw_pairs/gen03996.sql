SELECT id, event_type, event_time, session_id FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY event_type ORDER BY event_time) AS rnk FROM web_events WHERE duration_sec < 238) AS t WHERE rnk <= 5
