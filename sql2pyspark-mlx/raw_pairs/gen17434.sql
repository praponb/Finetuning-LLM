SELECT c.is_active, MIN(we.duration_sec) AS min_duration_sec FROM web_events we INNER JOIN sessions s ON we.session_id = s.id INNER JOIN customers c ON c.id = s.customer_id GROUP BY c.is_active;
