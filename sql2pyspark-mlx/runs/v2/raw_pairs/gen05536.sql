SELECT id, duration_sec, ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS rn FROM web_events WHERE event_type IN ('click', 'purchase')
