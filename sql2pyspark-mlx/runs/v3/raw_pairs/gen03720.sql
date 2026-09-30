SELECT we.page, s.started_at, c.name FROM web_events we JOIN sessions s ON we.session_id = s.id FULL OUTER JOIN customers c ON c.id = s.customer_id
