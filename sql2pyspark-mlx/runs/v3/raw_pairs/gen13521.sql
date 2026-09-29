SELECT we.page, we.event_type, s.customer_id, c.age FROM web_events we INNER JOIN sessions s ON s.id = we.session_id INNER JOIN customers c ON s.customer_id = c.id;
