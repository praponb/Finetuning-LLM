SELECT we.id, we.duration_sec, we.event_time, s.customer_id, c.country FROM web_events we INNER JOIN sessions s ON s.id = we.session_id INNER JOIN customers c ON c.id = s.customer_id ORDER BY id
